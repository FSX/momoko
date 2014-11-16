# -*- coding: utf-8 -*-
"""
momoko.connection
=================

Connection handling.

Copyright 2011-2014, Frank Smit & Zaar Hai.
MIT, see LICENSE for more details.
"""

import sys
if sys.version_info[0] >= 3:
    basestring = str

from functools import partial
from collections import deque
import datetime
from functools import wraps
from contextlib import contextmanager

import psycopg2
from psycopg2.extras import register_hstore as _psy_register_hstore
from psycopg2.extensions import POLL_OK, POLL_READ, POLL_WRITE, POLL_ERROR, TRANSACTION_STATUS_IDLE

from tornado import gen
from tornado.ioloop import IOLoop
from tornado.stack_context import wrap
from tornado.concurrent import Future

from .exceptions import PoolError

from .utils import log


# The dummy callback is used to keep the asynchronous cursor alive in case no
# callback has been specified. This will prevent the cursor from being garbage
# collected once, for example, ``Pool.execute`` has finished.
# Symptom: you'll get
#     InterfaceError: the asynchronous cursor has disappeared
# exceptions
def _dummy_callback(cursor, error):
    pass


class Pool(object):
    """
    Asynchronous connection pool.

    The pool manages database connections and passes operations to connections.

    See :py:class:`momoko.Connection` for documentation about the ``dsn``,
    ``connection_factory`` and ``cursor_factory`` parameters.
    These are used by the connection pool when a new connection is created.

    :param integer size: Amount of connections created upon initialization. Defaults to ``1``.
    :param integer max_size: Allow number of connection to grow under load up to given size. Defaults to ``size``.
    :param callable callback:
        A callable that's called after all the connections are created. Defaults to ``None``.
    :param ioloop: An instance of Tornado's IOLoop. Defaults to ``None``, ``IOLoop.instance()`` will be used.
    :param bool raise_connect_errors:
        Whether to raise exception if database connection fails. Set to ``False`` to enable
        automatic reconnection attempts. Defaults to ``True``.
    :param integer reconnect_interval:
        When using automatic reconnects, set minimum reconnect interval, in milliseconds,
        before retrying connection attempt. Don't set this value too low to prevent "banging"
        the database server with connection attempts. Defaults to ``500``.
    :param list setsession:
        List of intial sql commands to be executed once connection is established.
        If any of the commands failes, the connection will be closed.
        **NOTE:** The commands will be executed as one transaction block.
    """

    class Connections(object):
        def __init__(self, reconnect_interval, ioloop):
            self.ioloop = ioloop

            self.reconnect_interval = reconnect_interval
            self.last_connect_attempt_ts = ioloop.time()
            self.last_connect_attempt_success = False
            self.reconnect_in_progress = False

            self.empty()

        def empty(self):
            self.free = set()
            self.busy = set()
            self.dead = set()
            self.pending = set()
            self.waiting_queue = deque()

        def remove_pending(func):
            @wraps(func)
            def wrapper(self, conn):
                self.pending.discard(conn)
                func(self, conn)
            return wrapper

        def get_free(self):
            if not self.free:
                return
            conn = self.free.pop()
            self.busy.add(conn)
            return conn

        def return_busy(self, conn):
            if self.waiting_queue:
                self.waiting_queue.pop().set_result(conn)
            else:
                self.busy.remove(conn)
                self.free.add(conn)

        @remove_pending
        def add_free(self, conn):
            if self.waiting_queue:
                self.busy.add(conn)
                self.waiting_queue.pop().set_result(conn)
            else:
                self.free.add(conn)

        @remove_pending
        def add_dead(self, conn):
            self.dead.add(conn)
            self.busy.discard(conn)
            # free connections are most probably dead by now
            while self.free:
                self.dead.add(self.free.pop())

        def add_pending(self, conn):
            self.last_connect_attempt_ts = self.ioloop.time()
            self.reconnect_in_progress = True
            self.pending.add(conn)

        def get_alive(self):
            return self.free.union(self.busy)

        def close_alive(self):
            for conn in self.get_alive():
                if not conn.closed:
                    conn.close()

        @property
        def total(self):
            return len(self.free) + len(self.busy) + len(self.dead) + len(self.pending)

        def is_time_to_reconnect(self):
            now = self.ioloop.time()
            if not (self.last_connect_attempt_success or
                    now - self.last_connect_attempt_ts > self.reconnect_interval):
                return False
            return True

        def on_reconnect_complete(self, connection):
            if not connection.closed:
                self.add_free(connection)
            else:
                self.add_dead(connection)
            self.last_connect_attempt_success = not connection.closed
            self.reconnect_in_progress = False
            if not self.last_connect_attempt_success:
                self.abort_waiting_queue()

        def abort_waiting_queue(self):
            while self.waiting_queue:
                future = self.waiting_queue.pop()
                future.set_result(None)  # Send None to signify that all who waits should abort

    def __init__(self,
                 dsn,
                 connection_factory=None,
                 cursor_factory=None,
                 size=1,
                 max_size=None,
                 callback=None,
                 ioloop=None,
                 raise_connect_errors=True,
                 reconnect_interval=500,
                 setsession=[]):
        assert size > 0, "The connection pool size must be a number above 0."

        self.size = size
        self.max_size = max_size or size
        assert self.size <= self.max_size, "The connection pool max size must be of at least 'size'."

        self.dsn = dsn
        self.closed = False
        self.connection_factory = connection_factory
        self.cursor_factory = cursor_factory

        self.raise_connect_errors = raise_connect_errors

        self._ioloop = ioloop or IOLoop.instance()

        reconnect_interval = float(reconnect_interval)/1000  # the parameter is in milliseconds
        self._conns = self.Connections(reconnect_interval, self._ioloop)

        self.setsession = setsession
        self.connected = False

        self.server_version = None

        # Create connections
        def after_pool_creation(connection):
            if not self._conns.pending:  # all connections "connected" on way or the other
                if callback:
                    callback()

        for i in range(self.size):
            self._new(after_pool_creation)

    def _new(self, callback=None):
        conn = Connection()
        self._conns.add_pending(conn)
        conn.connect(self.dsn,
                     connection_factory=self.connection_factory,
                     cursor_factory=self.cursor_factory,
                     callback=partial(self._post_connect_callback, callback),
                     ioloop=self._ioloop,
                     setsession=self.setsession)

    def _post_connect_callback(self, callback, connection, error):
        if error:
            if not connection.closed:
                connection.close()
            if self.raise_connect_errors:
                raise error
            else:
                logger = log.error if self.log_connect_errors else log.info
                logger("Failed opening connection to database: %s", error)
        else:
            self.server_version = connection.connection.server_version

        self._conns.on_reconnect_complete(connection)
        log.debug("Connection attempt complete. Success: %s", self._conns.last_connect_attempt_success)

        if self._conns.last_connect_attempt_success:
            # Connection to db is OK. If we have waiting requests
            # and some dead connections, we can serve requests faster
            # if we reanimate dead connections
            num_conns_to_reconnect = min(len(self._conns.dead), len(self._conns.waiting_queue))
            for i in range(num_conns_to_reconnect):
                self._conns.dead.pop()
                self._new()

        self._stretch_if_needed()

        if callback:
            callback(connection)

    def _get_connection(self):

        log.debug("Getting connection")
        self.connected = True

        # if there are free connections - just return one
        connection = self._conns.get_free()
        if connection:
            return connection

        # if there are dead connections - try to reanimate them
        if self._conns.dead:
            if self._conns.is_time_to_reconnect():
                log.debug("Trying to reconnect dead connection")
                self._conns.dead.pop()
                self._new()
                return

        if self._conns.busy:
            # We may be maxed out here. Try to stretch if approprate
            self._stretch_if_needed(new_request=True)
            # At least some connections are alive, so wait for them
            log.debug("There are busy connections")
            return

        if self._conns.reconnect_in_progress:
            # We are connecting - wait more
            log.debug("Reconnect in progress")
            return

        log.debug("no connections are available or expected in near future")
        self.connected = False

    def _stretch_if_needed(self, new_request=False):
        if self._conns.total == self.max_size:
            return  # max size reached
        if self._conns.dead or self._conns.free:
            return  # no point to stretch if we heave free conns to use / dead conns to reanimate
        if not (new_request or (self._conns.waiting_queue and self._conns.busy)):
            return
        if self._conns.pending:
            if len(self._conns.pending) >= len(self._conns.waiting_queue) + int(new_request):
                return
        log.debug("Stretching pool")
        self._new()

    def _retry_action(self, method, callback, *args, **kwargs):
        action = partial(self._operate, method, callback, *args, **kwargs)
        future = Future()
        self._conns.waiting_queue.appendleft(future)

        def on_connection_available(future):
            connection = future.result()
            if not connection:
                log.debug("Aborting - as instructed")
                raise psycopg2.DatabaseError("No database connection available")
            action(connection=connection)
        return self._ioloop.add_future(future, on_connection_available)

    def _reconnect_and_retry(self, connection, method, callback, *args, **kwargs):
        self._conns.add_dead(connection)
        log.debug("Tried over dead connection. Retrying once")
        self._retry_action(method, callback, *args, **kwargs)
        self._conns.dead.pop()
        self._new()

    def _operate(self, method, callback, *args, **kwargs):

        connection = kwargs.pop("connection", None) or self._get_connection()
        if not connection:
            if self.connected:
                log.debug("No connection available right now - will try again later")
                return self._retry_action(method, callback, *args, **kwargs)
            else:
                log.debug("Aborting - not connected")
                raise psycopg2.DatabaseError("No database connection available")

        log.debug("Connection obtained, proceeding")

        if kwargs.pop("get_connection_only", False):
            return callback(connection, None)

        the_callback = partial(self._operate_callback, connection, method, callback, args, kwargs)
        method(connection, *args, callback=the_callback, **kwargs)

    def _operate_callback(self, connection, method, orig_callback, args, kwargs, *_args, **_kwargs):
        """
        Wrap real callback coming from invoker with our own one
        that will check connection status after the end of the call
        and recycle connection / retry operation
        """
        if connection.closed:
            self._reconnect_and_retry(connection, method, orig_callback, *args, **kwargs)
            return

        if not getattr(method, "_keep_connection", False):
            self._conns.return_busy(connection)

        if orig_callback:
            return orig_callback(*_args, **_kwargs)

    def ping(self, connection, callback=None):
        """
        Ping given connection object to make sure its alive (involves roundtrip to the database server).

        See :py:meth:`momoko.Connection.ping` for documentation about the details.
        """
        self._operate(Connection.ping, callback,
                      connection=connection)

    def transaction(self,
                    statements,
                    cursor_factory=None,
                    callback=None):
        """
        Run a sequence of SQL queries in a database transaction.

        See :py:meth:`momoko.Connection.transaction` for documentation about the
        parameters.
        """
        self._operate(Connection.transaction, callback,
                      statements, cursor_factory=cursor_factory)

    def execute(self,
                operation,
                parameters=(),
                cursor_factory=None,
                callback=None):
        """
        Prepare and execute a database operation (query or command).

        See :py:meth:`momoko.Connection.execute` for documentation about the
        parameters.
        """
        self._operate(Connection.execute, callback,
                      operation, parameters, cursor_factory=cursor_factory)

    def callproc(self,
                 procname,
                 parameters=(),
                 cursor_factory=None,
                 callback=None):
        """
        Call a stored database procedure with the given name.

        See :py:meth:`momoko.Connection.callproc` for documentation about the
        parameters.
        """
        self._operate(Connection.callproc, callback,
                      procname, parameters=parameters, cursor_factory=cursor_factory)

    def mogrify(self,
                operation,
                parameters=(),
                callback=None):
        """
        Return a query string after arguments binding.

        See :py:meth:`momoko.Connection.mogrify` for documentation about the
        parameters.
        """
        self._operate(Connection.mogrify, callback,
                      operation, parameters=parameters)

    def register_hstore(self, unicode=False, callback=None):
        """
        Register adapter and typecaster for ``dict-hstore`` conversions.

        See :py:meth:`momoko.Connection.register_hstore` for documentation about
        the parameters. This method has no ``globally`` parameter, because it
        already registers hstore to all the connections in the pool.
        """
        self._operate(Connection.register_hstore, callback,
                      globally=True, unicode=unicode)

    def getconn(self, ping=True, callback=None):
        """
        Acquire connection from the pool.

        You can then use this connection for subsequest queries.
        Just supply, for example, ``connection.execute`` instead of ``Pool.execute``
        to ``momoko.Op``.

        Make sure to return connection to the pool by calling :py:meth:`momoko.Pool.putconn`,
        otherwise the connection will remain forever-busy and you'll starvate your pool quickly.

        :param boolean ping:
            Whether to ping connection before returning it by executing :py:meth:`momoko.Pool.ping`.
        """
        def ping_callback(connection, error):
            self.ping(connection, callback)
        the_callback = ping_callback if ping else callback
        self._operate("getconn", the_callback, get_connection_only=True)

    def putconn(self, connection):
        """
        Retrun busy connection back to the pool.

        :param Connection connection:
            Connection object previously returned by :py:meth:`momoko.Pool.getconn`.
        """

        if connection.closed:
            self._conns.add_dead(connection)
        else:
            self._conns.return_busy(connection)

    @contextmanager
    def manage(self, connection):
        """
        Context manager that automatically returns connection to the pool.
        You can use it instead of :py:meth:`momoko.Pool.putconn`::

            connection = yield momoko.Op(self.db.getconn)
            with self.db.manage(connection):
                cursor = yield momoko.Op(connection.execute, "BEGIN")
                ...
        """
        assert connection in self._conns.busy, "Can not manage non-busy connection. Where did you get it from?"
        try:
            yield connection
        finally:
            self.putconn(connection)

    def close(self):
        """
        Close the connection pool.
        """
        if self.closed:
            raise PoolError('connection pool is already closed')

        self._conns.close_alive()
        self._conns.empty()
        self.closed = True

    log_connect_errors = True  # Unittest monkey patches it for silent output


class Connection(object):
    """
    Initiate an asynchronous connect.

    :param string dsn:
        A `Data Source Name`_ string containing one of the following values:

        * **dbname** - the database name
        * **user** - user name used to authenticate
        * **password** - password used to authenticate
        * **host** - database host address (defaults to UNIX socket if not provided)
        * **port** - connection port number (defaults to 5432 if not provided)

        Or any other parameter supported by PostgreSQL. See the PostgreSQL
        documentation for a complete list of supported parameters_.

    :param connection_factory:
        The ``connection_factory`` argument can be used to create non-standard
        connections. The class returned should be a subclass of `psycopg2.extensions.connection`_.
        See `Connection and cursor factories`_ for details. Defaults to ``None``.

    :param cursor_factory:
        The ``cursor_factory`` argument can be used to return non-standart cursor class
        The class returned should be a subclass of `psycopg2.extensions.cursor`_.
        See `Connection and cursor factories`_ for details. Defaults to ``None``.

    :param callable callback:
        A callable that's called after the connection is created. It accepts one
        paramater: an instance of :py:class:`momoko.Connection`. Defaults to ``None``.
    :param ioloop: An instance of Tornado's IOLoop. Defaults to ``None``.

    :param list setsession:
        List of intial sql commands to be executed once connection is established.
        If any of the commands failes, the connection will be closed.
        **NOTE:** The commands will be executed as one transaction block.

    .. _Data Source Name: http://en.wikipedia.org/wiki/Data_Source_Name
    .. _parameters: http://www.postgresql.org/docs/current/static/libpq-connect.html#LIBPQ-PQCONNECTDBPARAMS
    .. _psycopg2.extensions.connection: http://initd.org/psycopg/docs/connection.html#connection
    .. _Connection and cursor factories: http://initd.org/psycopg/docs/advanced.html#subclassing-cursor
    """
    def connect(self,
                dsn,
                connection_factory=None,
                cursor_factory=None,
                callback=None,
                ioloop=None,
                setsession=[]):
        log.info("Opening new database connection")

        kwargs = {"async": True}
        if connection_factory:
            kwargs["connection_factory"] = connection_factory
        if cursor_factory:
            kwargs["cursor_factory"] = cursor_factory

        self.connection = None
        try:
            self.connection = psycopg2.connect(dsn, **kwargs)
        except psycopg2.Error as error:
            if callback:
                callback(self, error)
                return
            else:
                raise
        self.fileno = self.connection.fileno()
        self._transaction_status = self.connection.get_transaction_status
        self.ioloop = ioloop or IOLoop.instance()

        self._on_connect_callback = partial(callback, self) if callback else None

        if setsession:
            self.callback = self._setsession_callback
            self.setsession = setsession
        else:
            self.callback = self._on_connect_callback

        self.ioloop.add_handler(self.fileno, self.io_callback, IOLoop.WRITE)

    def _setsession_callback(self, error):
        """Custom post-connect callback to trigger setsession commands execution in transaction"""
        if error:
            return self._on_connect_callback(error)
        log.debug("Running setsession commands")
        return self.transaction(self.setsession, callback=self._setsession_transaction_callback)

    def _setsession_transaction_callback(self, cursor, error):
        """
        Call back that check results of setsession transaction commands and
        call the real post_connect callback. Closes connection if transaction failed.
        """
        if error:
            log.debug("Closing connection since set session commands failed")
            self.close()
        self._on_connect_callback(error)

    def io_callback(self, fd=None, events=None):
        try:
            state = self.connection.poll()
        except (psycopg2.Warning, psycopg2.Error) as error:
            self.ioloop.remove_handler(self.fileno)
            if self.callback:
                self.callback(error)
        else:
            if state == POLL_OK:
                self.ioloop.remove_handler(self.fileno)
                if self.callback:
                    self.callback(None)
            elif state == POLL_READ:
                self.ioloop.update_handler(self.fileno, IOLoop.READ)
            elif state == POLL_WRITE:
                self.ioloop.update_handler(self.fileno, IOLoop.WRITE)
            else:
                raise psycopg2.OperationalError('poll() returned {0}'.format(state))

    def _catch_early_errors(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            callback = kwargs.get("callback", _dummy_callback)
            try:
                return func(self, *args, **kwargs)
            except Exception as error:
                callback(None, error)
        return wrapper

    def _keep_connection(func):
        """
        Use this decorator on Connection methods to hint the Pool to not
        release connection when operation is complete
        """
        func._keep_connection = True
        return func

    @_catch_early_errors
    @_keep_connection
    def ping(self, callback=None):
        """
        Make sure this connection is alive by executing SELECT 1 statement -
        i.e. roundtrip to the database.

        **NOTE:** On the contrary to other methods, callback function signature is
              ``callback(self, error)`` and not ``callback(cursor, error)``.

        **NOTE:** `callback` should always passed as keyword argument

        """
        cursor = self.connection.cursor()
        cursor.execute("SELECT 1")
        self.callback = partial(self._ping_callback, callback or _dummy_callback, cursor)
        self.ioloop.add_handler(self.fileno, self.io_callback, IOLoop.WRITE)

    def _ping_callback(self, callback, cursor, error):
        if not error:
            cursor.fetchall()
        return callback(self, error)

    @_catch_early_errors
    def execute(self,
                operation,
                parameters=(),
                cursor_factory=None,
                callback=None):
        """
        Prepare and execute a database operation (query or command).

        :param string operation: An SQL query.
        :param tuple/list parameters:
            A list or tuple with query parameters. See `Passing parameters to SQL queries`_
            for more information. Defaults to an empty tuple.
        :param cursor_factory:
            The ``cursor_factory`` argument can be used to create non-standard cursors.
            The class returned must be a subclass of `psycopg2.extensions.cursor`_.
            See `Connection and cursor factories`_ for details. Defaults to ``None``.
        :param callable callback:
            A callable that is executed when the query has finished. It must accept
            two positional parameters. The first one being the cursor and the second
            one ``None`` or an instance of an exception if an error has occurred,
            in that case the first parameter will be ``None``. Defaults to ``None``.
            **NOTE:** `callback` should always passed as keyword argument

        .. _Passing parameters to SQL queries: http://initd.org/psycopg/docs/usage.html#query-parameters
        .. _psycopg2.extensions.cursor: http://initd.org/psycopg/docs/extensions.html#psycopg2.extensions.cursor
        .. _Connection and cursor factories: http://initd.org/psycopg/docs/advanced.html#subclassing-cursor
        """
        kwargs = {"cursor_factory": cursor_factory} if cursor_factory else {}
        cursor = self.connection.cursor(**kwargs)
        cursor.execute(operation, parameters)
        self.callback = partial(callback or _dummy_callback, cursor)
        self.ioloop.add_handler(self.fileno, self.io_callback, IOLoop.WRITE)

    @_catch_early_errors
    def callproc(self,
                 procname,
                 parameters=(),
                 cursor_factory=None,
                 callback=None):
        """
        Call a stored database procedure with the given name.

        The sequence of parameters must contain one entry for each argument that
        the procedure expects. The result of the call is returned as modified copy
        of the input sequence. Input parameters are left untouched, output and
        input/output parameters replaced with possibly new values.

        The procedure may also provide a result set as output. This must then be
        made available through the standard `fetch*()`_ methods.

        :param string procname: The name of the database procedure.
        :param tuple/list parameters:
            A list or tuple with query parameters. See `Passing parameters to SQL queries`_
            for more information. Defaults to an empty tuple.
        :param cursor_factory:
            The ``cursor_factory`` argument can be used to create non-standard cursors.
            The class returned must be a subclass of `psycopg2.extensions.cursor`_.
            See `Connection and cursor factories`_ for details. Defaults to ``None``.
        :param callable callback:
            A callable that is executed when the query has finished. It must accept
            two positional parameters. The first one being the cursor and the second
            one ``None`` or an instance of an exception if an error has occurred,
            in that case the first parameter will be ``None``. Defaults to ``None``.
            **NOTE:** `callback` should always passed as keyword argument

        .. _fetch*(): http://initd.org/psycopg/docs/cursor.html#fetch
        .. _Passing parameters to SQL queries: http://initd.org/psycopg/docs/usage.html#query-parameters
        .. _psycopg2.extensions.cursor: http://initd.org/psycopg/docs/extensions.html#psycopg2.extensions.cursor
        .. _Connection and cursor factories: http://initd.org/psycopg/docs/advanced.html#subclassing-cursor
        """
        kwargs = {"cursor_factory": cursor_factory} if cursor_factory else {}
        cursor = self.connection.cursor(**kwargs)
        cursor.callproc(procname, parameters)
        self.callback = partial(callback or _dummy_callback, cursor)
        self.ioloop.add_handler(self.fileno, self.io_callback, IOLoop.WRITE)

    @_catch_early_errors
    def mogrify(self, operation, parameters=(), callback=None):
        """
        Return a query string after arguments binding.

        The string returned is exactly the one that would be sent to the database
        running the execute() method or similar.

        :param string operation: An SQL query.
        :param tuple/list parameters:
            A list or tuple with query parameters. See `Passing parameters to SQL queries`_
            for more information. Defaults to an empty tuple.
        :param callable callback:
            A callable that is executed when the query has finished. It must accept
            two positional parameters. The first one being the resulting query as
            a byte string and the second one ``None`` or an instance of an exception
            if an error has occurred. Defaults to ``None``.
            **NOTE:** `callback` should always passed as keyword argument

        .. _Passing parameters to SQL queries: http://initd.org/psycopg/docs/usage.html#query-parameters
        .. _Connection and cursor factories: http://initd.org/psycopg/docs/advanced.html#subclassing-cursor
        """
        cursor = self.connection.cursor()
        try:
            result = cursor.mogrify(operation, parameters)
            self.ioloop.add_callback(partial(callback or _dummy_callback, result, None))
        except (psycopg2.Warning, psycopg2.Error) as error:
            self.ioloop.add_callback(partial(callback or _dummy_callback, b'', error))

    def transaction(self,
                    statements,
                    cursor_factory=None,
                    callback=None):
        """
        Run a sequence of SQL queries in a database transaction.

        :param tuple/list statements:
            List or tuple containing SQL queries with or without parameters. An item
            can be a string (SQL query without parameters) or a tuple/list with two items,
            an SQL query and a tuple/list wuth parameters.

            See `Passing parameters to SQL queries`_ for more information.
        :param cursor_factory:
            The ``cursor_factory`` argument can be used to create non-standard cursors.
            The class returned must be a subclass of `psycopg2.extensions.cursor`_.
            See `Connection and cursor factories`_ for details. Defaults to ``None``.
        :param callable callback:
            A callable that is executed when the transaction has finished. It must accept
            two positional parameters. The first one being a list of cursors in the same
            order as the given statements and the second one ``None`` or an instance of
            an exception if an error has occurred, in that case the first parameter is
            an empty list. Defaults to ``None``.
            **NOTE:** `callback` should always passed as keyword argument

        .. _Passing parameters to SQL queries: http://initd.org/psycopg/docs/usage.html#query-parameters
        .. _psycopg2.extensions.cursor: http://initd.org/psycopg/docs/extensions.html#psycopg2.extensions.cursor
        .. _Connection and cursor factories: http://initd.org/psycopg/docs/advanced.html#subclassing-cursor
        """
        cursors = []
        queue = deque()
        callback = callback or _dummy_callback

        for statement in statements:
            if isinstance(statement, basestring):
                queue.append((statement, ()))
            else:
                queue.append(statement[:2])

        queue.appendleft(('BEGIN;', ()))
        queue.append(('COMMIT;', ()))

        def error_callback(statement_error, cursor, rollback_error):
            callback(None, rollback_error or statement_error)

        def exec_statement(cursor=None, error=None):
            if error:
                try:
                    self.execute('ROLLBACK;', callback=partial(error_callback, error))
                except psycopg2.Error as rollback_error:
                    error_callback(error, cursor, rollback_error)
                return
            if cursor:
                cursors.append(cursor)
            if not queue:
                callback(cursors[1:-1], None)
                return

            operation, parameters = queue.popleft()
            self.execute(operation, parameters, cursor_factory, callback=exec_statement)

        self.ioloop.add_callback(exec_statement)

    @_catch_early_errors
    def register_hstore(self, globally=False, unicode=False, callback=None):
        """
        Register adapter and typecaster for ``dict-hstore`` conversions.

        More information on the hstore datatype can be found on the
        Psycopg2 documentation_.

        :param boolean globally:
            Register the adapter globally, not only on this connection.
        :param boolean unicode:
            If ``True``, keys and values returned from the database will be ``unicode``
            instead of ``str``. The option is not available on Python 3.

        **NOTE:** `callback` should always passed as keyword argument

        .. _documentation: http://initd.org/psycopg/docs/extras.html#hstore-data-type
        """
        def _hstore_callback(cursor, error):
            oid, array_oid = cursor.fetchone()
            _psy_register_hstore(None, globally, unicode, oid, array_oid)

            if callback:
                callback(None, error)

        self.execute(
            "SELECT 'hstore'::regtype::oid, 'hstore[]'::regtype::oid",
            callback=_hstore_callback)

    def busy(self):
        """
        **(Deprecated)** Check if the connection is busy or not.
        """
        return self.connection.isexecuting() or (self.connection.closed == 0 and
                                                 self._transaction_status() != TRANSACTION_STATUS_IDLE)

    @property
    def closed(self):
        """
        Indicates whether the connection is closed or not.
        """
        # 0 = open, 1 = closed, 2 = 'something horrible happened'
        return self.connection.closed > 0 if self.connection else True

    def close(self):
        """
        Remove the connection from the IO loop and close it.
        """
        if self.connection:
            self.connection.close()
