# -*- coding: utf-8 -*-
"""
momoko.connection
=================

Connection handling.

Copyright 2011-2013 by Frank Smit.
MIT, see LICENSE for more details.
"""

from functools import partial
from collections import deque
import datetime
from functools import wraps

import psycopg2
from psycopg2.extras import register_hstore as _psy_register_hstore
from psycopg2.extensions import (connection as base_connection, cursor as base_cursor,
                                 POLL_OK, POLL_READ, POLL_WRITE, POLL_ERROR, TRANSACTION_STATUS_IDLE)

from tornado import gen
from tornado.ioloop import IOLoop
from tornado.stack_context import wrap
from tornado.concurrent import Future

from .exceptions import PoolError

from .utils import log


# The dummy callback is used to keep the asynchronous cursor alive in case no
# callback has been specified. This will prevent the cursor from being garbage
# collected once, for example, ``Pool.execute`` has finished.
def _dummy_callback(cursor, error):
    pass


class Pool(object):
    """
    Asynchronous connection pool.

    The pool manages database connections and passes operations to connections.

    See :py:class:`momoko.Connection` for documentation about the ``dsn`` and
    ``connection_factory`` parameters. These are used by the connection pool when
    a new connection is created.

    :param integer size: Amount of connections created upon initialization. Defaults to ``1``.
    :param callable callback:
        A callable that's called after all the connections are created. Defaults to ``None``.
    :param ioloop: An instance of Tornado's IOLoop. Defaults to ``None``.
    :param bool raise_connect_errors:
        Whether to raise exception if database connection fails. Set to ``False`` to enable
        automatic reconnection attempts. Defaults to ``True``.
    :param integer reconnect_interval:
        When using automatic reconnets, set minimum reconnect interval, in milliseconds,
        before retrying connection attempt. Don't set this value too low to prevent "banging"
        the database server with connection attempts. Defaults to ``500``.
    :param list set_session:
        List of intial sql commands to be executed once connection is established.
        If any of the commands failes, the connection will be closed.
        NOTE: The commands will be executed as one transaction block.
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
                 size=1,
                 max_size=None,
                 callback=None,
                 ioloop=None,
                 raise_connect_errors=True,
                 reconnect_interval=500,
                 set_session=[]):
        assert size > 0, "The connection pool size must be a number above 0."

        self.size = size
        self.max_size = max_size or size
        assert self.size <= self.max_size, "The connection pool max size must be of at least 'size'."

        self.dsn = dsn
        self.closed = False
        self.connection_factory = connection_factory
        self.raise_connect_errors = raise_connect_errors

        self._ioloop = ioloop or IOLoop.instance()

        reconnect_interval = float(reconnect_interval)/1000  # the parameter is in milliseconds
        self._conns = self.Connections(reconnect_interval, self._ioloop)

        self.set_session = set_session
        self.connected = False

        # Create connections
        def after_pool_creation(connection):
            if not self._conns.pending:  # all connections "connected" on way or the other
                if callback:
                    callback()

        for i in range(self.size):
            self._new(after_pool_creation)

    def _new(self, callback=None):
        def post_connect_callback(connection, error):
            if error:
                connection.close()
                if self.raise_connect_errors:
                    raise error
                else:
                    log.error("Failed opening connection to database: %s", error)

            self._conns.on_reconnect_complete(connection)
            log.debug("Connection attempt complete. Success: %s", self._conns.last_connect_attempt_success)

            if self._conns.last_connect_attempt_success:
                # Connection to db is OK. If we have waiting requests
                # and some dead conncetions, we can serve requests faster
                # if we reanimate dead connections
                num_conns_to_reconnect = min(len(self._conns.dead), len(self._conns.waiting_queue))
                for i in range(num_conns_to_reconnect):
                    self._conns.dead.pop()
                    self._new()
            self._stretch_if_needed()
            if callback:
                callback(connection)

        conn = Connection()
        self._conns.add_pending(conn)
        conn.connect(self.dsn, self.connection_factory,
                     post_connect_callback, self._ioloop, self.set_session)

    def _get_connection(self):

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
            # We may be maxed out here. Try to strach if approprate
            self._stretch_if_needed()
            # At least some connections are alive, so wait for them
            log.debug("There are busy connections")
            return

        if self._conns.reconnect_in_progress:
            # We are connecting - wait more
            log.debug("Reconnect in progress")
            return

        log.debug("no connections are available or expected in near future")
        self.connected = False

    def _stretch_if_needed(self):
        if self._conns.total < self.max_size and not (self._conns.dead or self._conns.free):
            log.debug("stretching pool")
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

        def conn_checker_callback_wrapper(callback):
            """
            Wrap real callback coming from invoker with our own one
            that will check connection status after the end of the call
            and recycle connection / retry operation
            """

            # needs to be a list since we can not modify the var itself in inner scope
            # http://eli.thegreenplace.net/2011/05/15/understanding-unboundlocalerror-in-python/
            attempt = [1]

            def inner(*_args, **_kwargs):
                if connection.closed:
                    self._conns.add_dead(connection)
                    if attempt[0] == 1:
                        attempt[0] += 1
                        log.debug("Tried over dead connection. Retrying once")
                        self._retry_action(method, callback, *args, **kwargs)
                        self._conns.dead.pop()
                        self._new()
                        return
                else:
                    self._conns.return_busy(connection)
                return callback(*_args, **_kwargs)
            return wrap(inner)

        callback = conn_checker_callback_wrapper(callback)
        getattr(connection, method)(*args, callback=callback, **kwargs)

    def transaction(self,
                    statements,
                    cursor_factory=None,
                    callback=None):
        """
        Run a sequence of SQL queries in a database transaction.

        See :py:meth:`momoko.Connection.transaction` for documentation about the
        parameters.
        """
        self._operate("transaction", callback,
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
        self._operate("execute", callback,
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
        self._operate("callproc", callback,
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
        self._operate("mogrify", callback,
                      operation, parameters=parameters)

    def register_hstore(self, unicode=False, callback=None):
        """
        Register adapter and typecaster for ``dict-hstore`` conversions.

        See :py:meth:`momoko.Connection.register_hstore` for documentation about
        the parameters. This method has no ``globally`` parameter, because it
        already registers hstore to all the connections in the pool.
        """
        self._operate("register_hstore", callback,
                      globally=True, unicode=unicode)

    def close(self):
        """
        Close the connection pool.
        """
        if self.closed:
            raise PoolError('connection pool is already closed')

        self._conns.close_alive()
        self._conns.empty()
        self.closed = True


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

    :param callable callback:
        A callable that's called after the connection is created. It accepts one
        paramater: an instance of :py:class:`momoko.Connection`. Defaults to ``None``.
    :param ioloop: An instance of Tornado's IOLoop. Defaults to ``None``.

    :param list set_session:
        List of intial sql commands to be executed once connection is established.
        If any of the commands failes, the connection will be closed.
        NOTE: The commands will be executed as one transaction block.

    .. _Data Source Name: http://en.wikipedia.org/wiki/Data_Source_Name
    .. _parameters: http://www.postgresql.org/docs/current/static/libpq-connect.html#LIBPQ-PQCONNECTDBPARAMS
    .. _psycopg2.extensions.connection: http://initd.org/psycopg/docs/connection.html#connection
    .. _Connection and cursor factories: http://initd.org/psycopg/docs/advanced.html#subclassing-cursor
    """
    def connect(self,
                dsn,
                connection_factory=None,
                callback=None,
                ioloop=None,
                set_session=[]):
        log.info("Opening new database connection")
        self.connection = psycopg2.connect(dsn, async=1,
                                           connection_factory=connection_factory or base_connection)
        self.fileno = self.connection.fileno()
        self._transaction_status = self.connection.get_transaction_status
        self.ioloop = ioloop or IOLoop.instance()

        self._on_connect_callback = partial(callback, self) if callback else None

        if set_session:
            self.callback = self._set_session_callback
            self.set_session = set_session
        else:
            self.callback = self._on_connect_callback

        self.ioloop.add_handler(self.fileno, self.io_callback, IOLoop.WRITE)

    def _set_session_callback(self, error):
        """Custom post-connect callback to trigger set_session commands execution in transaction"""
        if error:
            return self._on_connect_callback(error)
        log.debug("Running set_session commands")
        return self.transaction(self.set_session, callback=self._set_session_transaction_callback)

    def _set_session_transaction_callback(self, cursor, error):
        """
        Call back that check results of set_session transaction commands and
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

        .. _Passing parameters to SQL queries: http://initd.org/psycopg/docs/usage.html#query-parameters
        .. _psycopg2.extensions.cursor: http://initd.org/psycopg/docs/extensions.html#psycopg2.extensions.cursor
        .. _Connection and cursor factories: http://initd.org/psycopg/docs/advanced.html#subclassing-cursor
        """
        cursor = self.connection.cursor(cursor_factory=cursor_factory or base_cursor)
        cursor.execute(operation, parameters)
        self.callback = partial(callback or _dummy_callback, cursor)
        self.ioloop.add_handler(self.fileno, self.io_callback, IOLoop.WRITE)

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

        .. _fetch*(): http://initd.org/psycopg/docs/cursor.html#fetch
        .. _Passing parameters to SQL queries: http://initd.org/psycopg/docs/usage.html#query-parameters
        .. _psycopg2.extensions.cursor: http://initd.org/psycopg/docs/extensions.html#psycopg2.extensions.cursor
        .. _Connection and cursor factories: http://initd.org/psycopg/docs/advanced.html#subclassing-cursor
        """
        cursor = self.connection.cursor(cursor_factory=cursor_factory or base_cursor)
        cursor.callproc(procname, parameters)
        self.callback = partial(callback or _dummy_callback, cursor)
        self.ioloop.add_handler(self.fileno, self.io_callback, IOLoop.WRITE)

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

        .. _Passing parameters to SQL queries: http://initd.org/psycopg/docs/usage.html#query-parameters
        .. _psycopg2.extensions.cursor: http://initd.org/psycopg/docs/extensions.html#psycopg2.extensions.cursor
        .. _Connection and cursor factories: http://initd.org/psycopg/docs/advanced.html#subclassing-cursor
        """
        cursors = []
        queue = deque()
        callback = callback or _dummy_callback

        for statement in statements:
            if isinstance(statement, str):
                queue.append((statement, ()))
            else:
                queue.append(statement[:2])

        queue.appendleft(('BEGIN;', ()))
        queue.append(('COMMIT;', ()))

        def exec_statement(cursor=None, error=None):
            if error:
                self.execute('ROLLBACK;', callback=partial(error_callback, error))
                return
            if cursor:
                cursors.append(cursor)
            if not queue:
                callback(cursors[1:-1], None)
                return

            operation, parameters = queue.popleft()
            self.execute(operation, parameters, cursor_factory, exec_statement)

        def error_callback(statement_error, cursor, rollback_error):
            callback(None, rollback_error or statement_error)

        self.ioloop.add_callback(exec_statement)

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
        Check if the connection is busy or not.
        """
        return self.connection.isexecuting() or (self.connection.closed == 0 and
                                                 self._transaction_status() != TRANSACTION_STATUS_IDLE)

    @property
    def closed(self):
        """
        Indicates whether the connection is closed or not.
        """
        # 0 = open, 1 = closed, 2 = 'something horrible happened'
        return self.connection.closed > 0

    def close(self):
        """
        Remove the connection from the IO loop and close it.
        """
        self.connection.close()
