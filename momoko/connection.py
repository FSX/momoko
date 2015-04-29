# -*- coding: utf-8 -*-
"""
momoko.connection
=================

Connection handling.

Copyright 2011-2014, Frank Smit & Zaar Hai.
MIT, see LICENSE for more details.
"""

from __future__ import print_function

import sys
if sys.version_info[0] >= 3:
    basestring = str

import logging
from functools import partial
from collections import deque
import datetime
from functools import wraps
from contextlib import contextmanager

import psycopg2
from psycopg2.extras import register_hstore as _psy_register_hstore
from psycopg2.extras import register_json as _psy_register_json
from psycopg2.extensions import POLL_OK, POLL_READ, POLL_WRITE, POLL_ERROR

from tornado import gen
from tornado.ioloop import IOLoop
from tornado.concurrent import chain_future, Future

from .exceptions import PoolError, PartiallyConnectedError

log = logging.getLogger('momoko')


class ConnectionContainer(object):
    """
    Helper class that stores connecttions according to their state
    """
    def __init__(self):
        self.empty()

    def empty(self):
        self.free = set()
        self.busy = set()
        self.dead = set()
        self.pending = set()
        self.waiting_queue = deque()

    def add_free(self, conn):
        self.pending.discard(conn)
        log.debug("Handling free connection %s", conn.fileno)

        if not self.waiting_queue:
            log.debug("No outstanding requests - adding to free pool")
            self.free.add(conn)
            return

        log.debug("There are outstanding requests - resumed future from waiting queue")
        self.busy.add(conn)
        future = self.waiting_queue.pop()
        future.set_result(conn)

    def add_dead(self, conn):
        log.debug("Adding dead connection")
        self.pending.discard(conn)
        self.dead.add(conn)

    def acquire(self):
        """Occupy free connection"""
        future = Future()
        if self.free:
            conn = self.free.pop()
            self.busy.add(conn)
            future.set_result(conn)
            log.debug("Acquired free connection %s", conn.fileno)
            return future
        elif self.busy:
            log.debug("No free connections, and some are busy - put in waiting queue")
            self.waiting_queue.appendleft(future)
            return future
        else:
            log.debug("All connections are dead")
            return None

    def release(self, conn):
        log.debug("About to release connection %s", conn.fileno)
        assert conn in self.busy, "Tried to release non-busy connection"
        self.busy.remove(conn)
        if conn.closed:
            self.dead.add(conn)
        else:
            self.add_free(conn)

    def abort_waiting_queue(self, error):
        while self.waiting_queue:
            future = self.waiting_queue.pop()
            future.set_exception(error)

    def close_alive(self):
        for conn in self.free.union(self.busy):
            if not conn.closed:
                conn.close()

    @property
    def all_dead(self):
        return not (self.free or self.busy)

    @property
    def total(self):
        return len(self.free) + len(self.busy) + len(self.dead) + len(self.pending)


class Pool(object):
    def __init__(self,
                 dsn,
                 connection_factory=None,
                 cursor_factory=None,
                 size=1,
                 max_size=None,
                 ioloop=None,
                 raise_connect_errors=True,
                 reconnect_interval=500,
                 setsession=()):

        assert size > 0, "The connection pool size must be a number above 0."

        self.size = size
        self.max_size = max_size or size
        assert self.size <= self.max_size, "The connection pool max size must be of at least 'size'."

        self.dsn = dsn
        self.connection_factory = connection_factory
        self.cursor_factory = cursor_factory
        self.raise_connect_errors = raise_connect_errors
        self.reconnect_interval = float(reconnect_interval)/1000  # the parameter is in milliseconds
        self.setsession = setsession

        self.connected = False
        self.closed = False
        self.server_version = None

        self.ioloop = ioloop or IOLoop.instance()

        self.conns = ConnectionContainer()

        self._last_connect_time = 0
        self._no_conn_availble_error = psycopg2.DatabaseError("No database connection available")

    def connect(self):
        """
        Returns future that resolves to this Pool object.

        If some connection failed to connected, raises :py:meth:`momoko.PartiallyConnectedError`
        if self.raise_connect_errors is true.
        """
        future = Future()
        pending = [self.size-1]

        def on_connect(fut):
            if pending[0]:
                pending[0] -= 1
                return
            # all connection attemts are complete
            if self.conns.dead and self.raise_connect_errors:
                ecp = PartiallyConnectedError("%s connection(s) failed to connect" % len(self.conns.dead))
                future.set_exception(ecp)
            else:
                future.set_result(self)
            log.debug("All initial connection requests complete")

        for i in range(self.size):
            self.ioloop.add_future(self._new_connection(), on_connect)

        return future

    def getconn(self, ping=True):
        """
        Acquire connection from the pool.

        You can then use this connection for subsequest queries.
        Just use ``connection.execute`` instead of ``Pool.execute``.

        Make sure to return connection to the pool by calling :py:meth:`momoko.Pool.putconn`,
        otherwise the connection will remain forever-busy and you'll starvate your pool quickly.

        Returns future that resolves to the acquired connection object.

        :param boolean ping:
            Whether to ping the connection before returning it by executing :py:meth:`momoko.Connection.ping`.
        """
        rv = self.conns.acquire()
        if isinstance(rv, Future):
            self._reanimate_and_stretch_if_needed()
            future = rv
        else:
            # Else, all connections are dead
            assert len(self.conns.pending) == 0, "BUG! should be no pending connection"

            future = Future()

            def on_reanimate_done(fut):
                if self.conns.all_dead:
                    future.set_exception(self._no_conn_availble_error)
                    return
                f = self.conns.acquire()
                assert isinstance(f, Future)
                chain_future(f, future)

            self.ioloop.add_future(self._reanimate(), on_reanimate_done)

        if not ping:
            return future
        else:
            return self._ping_future_connection(future)

    def putconn(self, connection):
        """
        Retrun busy connection back to the pool.

        **NOTE:** This is a synchronous method.

        :param Connection connection:
            Connection object previously returned by :py:meth:`momoko.Pool.getconn`.
        """

        assert connection in self.conns.busy
        self.conns.release(connection)

        if self.conns.all_dead:
            self.conns.abort_waiting_queue(self._no_conn_availble_error)

    @contextmanager
    def manage(self, connection):
        """
        Context manager that automatically returns connection to the pool.
        You can use it instead of :py:meth:`momoko.Pool.putconn`::

            connection = yield self.db.getconn()
            with self.db.manage(connection):
                cursor = yield connection.execute("BEGIN")
                ...
        """
        assert connection in self.conns.busy, "Can not manage non-busy connection. Where did you get it from?"
        try:
            yield connection
        finally:
            self.putconn(connection)

    def ping(self):
        """
        Make sure this connection is alive by executing SELECT 1 statement -
        i.e. roundtrip to the database.

        See :py:meth:`momoko.Connection.ping` for documentation about the
        parameters.
        """
        return self._operate(Connection.ping)

    def execute(self, *args, **kwargs):
        """
        Prepare and execute a database operation (query or command).

        See :py:meth:`momoko.Connection.execute` for documentation about the
        parameters.
        """
        return self._operate(Connection.execute, args, kwargs)

    def callproc(self, *args, **kwargs):
        """
        Call a stored database procedure with the given name.

        See :py:meth:`momoko.Connection.callproc` for documentation about the
        parameters.
        """
        return self._operate(Connection.callproc, args, kwargs)

    def transaction(self, *args, **kwargs):
        """
        Run a sequence of SQL queries in a database transaction.

        See :py:meth:`momoko.Connection.transaction` for documentation about the
        parameters.
        """
        return self._operate(Connection.transaction, args, kwargs)

    def mogrify(self, *args, **kwargs):
        """
        Return a query string after arguments binding.

        **NOTE:** This is NOT a synchronous method (contary to `momoko.Connection.mogrify`)
        - it asynchronously waits for available connection. For performance
        reasons, its better to create dedicated :py:meth:`momoko.Connection`
        object and use it directly for mogrification, this operation does not
        imply any real operation on the database server.

        See :py:meth:`momoko.Connection.mogrify` for documentation about the
        parameters.
        """
        return self._operate(Connection.mogrify, args, kwargs, async=False)

    def register_hstore(self, *args, **kwargs):
        """
        Register adapter and typecaster for ``dict-hstore`` conversions.

        See :py:meth:`momoko.Connection.register_hstore` for documentation about
        the parameters. This method has no ``globally`` parameter, because it
        already registers hstore to all the connections in the pool.
        """
        kwargs["globally"] = True
        return self._operate(Connection.register_hstore, args, kwargs)

    def register_json(self, *args, **kwargs):
        """
        Create and register typecasters converting ``json`` type to Python objects.

        See :py:meth:`momoko.Connection.register_json` for documentation about
        the parameters. This method has no ``globally`` parameter, because it
        already registers json to all the connections in the pool.
        """
        kwargs["globally"] = True
        return self._operate(Connection.register_json, args, kwargs)

    def close(self):
        """
        Close the connection pool.

        **NOTE:** This is a synchronous method.
        """
        if self.closed:
            raise PoolError('connection pool is already closed')

        self.conns.close_alive()
        self.conns.empty()
        self.closed = True

    def _operate(self, method, args=(), kwargs=None, async=True, keep=False, connection=None):
        kwargs = kwargs or {}
        future = Future()

        retry = []

        def when_avaialble(fut):
            try:
                conn = fut.result()
            except psycopg2.Error as error:
                future.set_exc_info(sys.exc_info())
                if retry:
                    self.putconn(retry[0])
                return

            log.debug("Obtained connection: %s", conn.fileno)
            try:
                future_or_result = method(conn, *args, **kwargs)
            except psycopg2.Error as error:
                if conn.closed:
                    if not retry:
                        retry.append(conn)
                        self.ioloop.add_future(conn.connect(), when_avaialble)
                        return
                    else:
                        future.set_exception(self._no_conn_availble_error)
                else:
                    future.set_exc_info(sys.exc_info())
                log.debug(2)
                self.putconn(conn)
                return

            if not async:
                future.set_result(future_or_result)
                log.debug(3)
                self.putconn(conn)
                return

            chain_future(future_or_result, future)
            if not keep:
                future.add_done_callback(lambda f: self.putconn(conn))

        if not connection:
            self.ioloop.add_future(self.getconn(ping=False), when_avaialble)
        else:
            f = Future()
            f.set_result(connection)
            when_avaialble(f)
        return future

    def _reanimate(self):
        assert self.conns.dead, "BUG: dont' call reanimate when there is no one to reanimate"

        future = Future()

        if self.ioloop.time() - self._last_connect_time < self.reconnect_interval:
            log.debug("Not reconnecting - too soon")
            future.set_result(None)
            return future

        pending = [len(self.conns.dead)-1]

        def on_connect(fut):
            if pending[0]:
                pending[0] -= 1
                return
            future.set_result(None)

        while self.conns.dead:
            conn = self.conns.dead.pop()
            self.ioloop.add_future(self._connect_one(conn), on_connect)

        return future

    def _reanimate_and_stretch_if_needed(self):
        if self.conns.dead:
            self._reanimate()
            return

        if self.conns.total == self.max_size:
            return  # max size reached
        if self.conns.free:
            return  # no point to stretch if there are free connections
        if self.conns.pending:
            if len(self.conns.pending) >= len(self.conns.waiting_queue):
                return  # there are enough outstanding connection requests

        log.debug("Stretching pool")
        self._new_connection()

    def _new_connection(self):
        log.debug("Spawning new connection")
        conn = Connection(self.dsn,
                          connection_factory=self.connection_factory,
                          cursor_factory=self.cursor_factory,
                          ioloop=self.ioloop,
                          setsession=self.setsession)
        return self._connect_one(conn)

    def _connect_one(self, conn):
        future = Future()
        self.conns.pending.add(conn)

        def on_connect(fut):
            try:
                fut.result()
            except psycopg2.Error as error:
                self.conns.add_dead(conn)
            else:
                self.conns.add_free(conn)
                self.server_version = conn.server_version
            self._last_connect_time = self.ioloop.time()
            future.set_result(conn)

        self.ioloop.add_future(conn.connect(), on_connect)
        return future

    def _ping_future_connection(self, conn_future):
        ping_future = Future()

        def on_connection_available(fut):
            conn = fut.result()

            def on_ping_done(ping_fut):
                try:
                    ping_fut.result()
                except psycopg2.Error as error:
                    ping_future.set_exc_info(error)
                    self.putconn(conn)
                else:
                    ping_future.set_result(conn)

            f = self._operate(Connection.ping, keep=True, connection=conn)
            self.ioloop.add_future(f, on_ping_done)

        self.ioloop.add_future(conn_future, on_connection_available)

        return ping_future


class Connection(object):
    """
    Asynchronous connection object. All its methods are
    asynchronous unless stated otherwide in method description.

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

    :param list setsession:
        List of intial sql commands to be executed once connection is established.
        If any of the commands failes, the connection will be closed.
        **NOTE:** The commands will be executed as one transaction block.

    .. _Data Source Name: http://en.wikipedia.org/wiki/Data_Source_Name
    .. _parameters: http://www.postgresql.org/docs/current/static/libpq-connect.html#LIBPQ-PQCONNECTDBPARAMS
    .. _psycopg2.extensions.connection: http://initd.org/psycopg/docs/connection.html#connection
    .. _Connection and cursor factories: http://initd.org/psycopg/docs/advanced.html#subclassing-cursor
    """
    def __init__(self,
                 dsn,
                 connection_factory=None,
                 cursor_factory=None,
                 ioloop=None,
                 setsession=()):

        self.dsn = dsn
        self.connection_factory = connection_factory
        self.cursor_factory = cursor_factory
        self.ioloop = ioloop or IOLoop.instance()
        self.setsession = setsession

    def connect(self):
        """
        Initiate asynchronous connect.
        Returns future that resolves to this connection object.
        """
        kwargs = {"async": True}
        if self.connection_factory:
            kwargs["connection_factory"] = self.connection_factory
        if self.cursor_factory:
            kwargs["cursor_factory"] = self.cursor_factory

        future = Future()

        self.connection = None
        try:
            self.connection = psycopg2.connect(self.dsn, **kwargs)
        except psycopg2.Error as error:
            future.set_exc_info(sys.exc_info())
            return future

        self.fileno = self.connection.fileno()

        if self.setsession:
            on_connect_future = Future()

            def on_connect(on_connect_future):
                self.ioloop.add_future(self.transaction(self.setsession), lambda x: future.set_result(self))

            self.ioloop.add_future(on_connect_future, on_connect)
            callback = partial(self._io_callback, on_connect_future, self)
        else:
            callback = partial(self._io_callback, future, self)

        self.ioloop.add_handler(self.fileno, callback, IOLoop.WRITE)
        self.ioloop.add_future(future, self._set_server_version)

        return future

    def _set_server_version(self, future):
        if future.exception():
            return
        self.server_version = self.connection.server_version

    def _io_callback(self, future, result, fd=None, events=None):
        try:
            state = self.connection.poll()
        except (psycopg2.Warning, psycopg2.Error) as error:
            self.ioloop.remove_handler(self.fileno)
            future.set_exc_info(sys.exc_info())
        else:
            if state == POLL_OK:
                self.ioloop.remove_handler(self.fileno)
                future.set_result(result)
            elif state == POLL_READ:
                self.ioloop.update_handler(self.fileno, IOLoop.READ)
            elif state == POLL_WRITE:
                self.ioloop.update_handler(self.fileno, IOLoop.WRITE)
            else:
                future.set_exception(psycopg2.OperationalError('poll() returned {0}'.format(state)))

    def ping(self):
        """
        Make sure this connection is alive by executing SELECT 1 statement -
        i.e. roundtrip to the database.

        Returns future. If it resolves sucessfully - the connection is alive (or dead otherwise).
        """
        return self.execute("SELECT 1 AS ping")

    def execute(self,
                operation,
                parameters=(),
                cursor_factory=None):
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

        Returns future that resolves to cursor object containing result.

        .. _Passing parameters to SQL queries: http://initd.org/psycopg/docs/usage.html#query-parameters
        .. _psycopg2.extensions.cursor: http://initd.org/psycopg/docs/extensions.html#psycopg2.extensions.cursor
        .. _Connection and cursor factories: http://initd.org/psycopg/docs/advanced.html#subclassing-cursor
        """
        kwargs = {"cursor_factory": cursor_factory} if cursor_factory else {}
        cursor = self.connection.cursor(**kwargs)
        cursor.execute(operation, parameters)

        future = Future()
        callback = partial(self._io_callback, future, cursor)
        self.ioloop.add_handler(self.fileno, callback, IOLoop.WRITE)
        return future

    def callproc(self,
                 procname,
                 parameters=(),
                 cursor_factory=None):
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

        Returns future that resolves to cursor object containing result.

        .. _fetch*(): http://initd.org/psycopg/docs/cursor.html#fetch
        .. _Passing parameters to SQL queries: http://initd.org/psycopg/docs/usage.html#query-parameters
        .. _psycopg2.extensions.cursor: http://initd.org/psycopg/docs/extensions.html#psycopg2.extensions.cursor
        .. _Connection and cursor factories: http://initd.org/psycopg/docs/advanced.html#subclassing-cursor
        """
        kwargs = {"cursor_factory": cursor_factory} if cursor_factory else {}
        cursor = self.connection.cursor(**kwargs)
        cursor.callproc(procname, parameters)

        future = Future()
        callback = partial(self._io_callback, future, cursor)
        self.ioloop.add_handler(self.fileno, callback, IOLoop.WRITE)
        return future

    def mogrify(self, operation, parameters=()):
        """
        Return a query string after arguments binding.

        The string returned is exactly the one that would be sent to the database
        running the execute() method or similar.

        **NOTE:** This is a synchronous method.

        :param string operation: An SQL query.
        :param tuple/list parameters:
            A list or tuple with query parameters. See `Passing parameters to SQL queries`_
            for more information. Defaults to an empty tuple.

        .. _Passing parameters to SQL queries: http://initd.org/psycopg/docs/usage.html#query-parameters
        .. _Connection and cursor factories: http://initd.org/psycopg/docs/advanced.html#subclassing-cursor
        """
        cursor = self.connection.cursor()
        return cursor.mogrify(operation, parameters)

    def transaction(self,
                    statements,
                    cursor_factory=None,
                    auto_rollback=True):
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
        :param bool auto_rollback:
            If one of the transaction statements failes, try to automatically
            execute ROLLBACK to abort the transaction. If ROLLBACK fails, it would
            not be raised, but only logged.

        Returns future that resolves to ``list`` of cursors. Each cursor contains the result
        of the corresponding transaction statement.

        .. _Passing parameters to SQL queries: http://initd.org/psycopg/docs/usage.html#query-parameters
        .. _psycopg2.extensions.cursor: http://initd.org/psycopg/docs/extensions.html#psycopg2.extensions.cursor
        .. _Connection and cursor factories: http://initd.org/psycopg/docs/advanced.html#subclassing-cursor
        """
        cursors = []
        transaction_future = Future()

        queue = self._statement_generator(statements)

        def exec_statements(future):
            try:
                cursor = future.result()
                cursors.append(cursor)
            except Exception as error:
                if not auto_rollback:
                    transaction_future.set_exc_info(sys.exc_info())
                else:
                    self._rollback(transaction_future, error)
                return

            try:
                operation, parameters = next(queue)
            except StopIteration:
                transaction_future.set_result(cursors[1:-1])
                return

            f = self.execute(operation, parameters, cursor_factory)
            self.ioloop.add_future(f, exec_statements)

        self.ioloop.add_future(self.execute("BEGIN;"), exec_statements)
        return transaction_future

    def _statement_generator(self, statements):
        for statement in statements:
            if isinstance(statement, basestring):
                yield (statement, ())
            else:
                yield statement[:2]
        yield ('COMMIT;', ())

    def _rollback(self, transaction_future, error):
        def rollback_callback(rb_future):
            try:
                rb_future.result()
            except Exception as rb_error:
                log.warn("Failed to ROLLBACK transaction %s", rb_error)
            transaction_future.set_exception(error)
        self.ioloop.add_future(self.execute("ROLLBACK;"), rollback_callback)

    def _register(self, future, registrator, fut):
        try:
            cursor = fut.result()
        except Exception as error:
            future.set_exc_info(sys.exc_info())
            return

        oid, array_oid = cursor.fetchone()
        registrator(oid, array_oid)
        future.set_result(None)

    def register_hstore(self, globally=False, unicode=False):
        """
        Register adapter and typecaster for ``dict-hstore`` conversions.

        More information on the hstore datatype can be found on the
        Psycopg2 |hstoredoc|_.

        :param boolean globally:
            Register the adapter globally, not only on this connection.
        :param boolean unicode:
            If ``True``, keys and values returned from the database will be ``unicode``
            instead of ``str``. The option is not available on Python 3.

        Returns future that resolves to ``None``.

        .. |hstoredoc| replace:: documentation

        .. _hstoredoc: http://initd.org/psycopg/docs/extras.html#hstore-data-type
        """
        future = Future()
        registrator = partial(_psy_register_hstore, None, globally, unicode)
        callback = partial(self._register, future, registrator)
        self.ioloop.add_future(self.execute(
            "SELECT 'hstore'::regtype::oid, 'hstore[]'::regtype::oid",
        ), callback)

        return future

    def register_json(self, globally=False, loads=None):
        """
        Create and register typecasters converting ``json`` type to Python objects.

        More information on the json datatype can be found on the Psycopg2 |regjsondoc|_.

        :param boolean globally:
            Register the adapter globally, not only on this connection.
        :param function loads:
            The function used to parse the data into a Python object.  If ``None``
            use ``json.loads()``, where ``json`` is the module chosen according to
            the Python version.  See psycopg2.extra docs.

        Returns future that resolves to ``None``.

        .. |regjsondoc| replace:: documentation

        .. _regjsondoc: http://initd.org/psycopg/docs/extras.html#json-adaptation
        """
        future = Future()
        registrator = partial(_psy_register_json, None, globally, loads)
        callback = partial(self._register, future, registrator)
        self.ioloop.add_future(self.execute(
            "SELECT 'json'::regtype::oid, 'json[]'::regtype::oid"
        ), callback)

        return future

    @property
    def closed(self):
        """
        Indicates whether the connection is closed or not.
        """
        # 0 = open, 1 = closed, 2 = 'something horrible happened'
        return self.connection.closed > 0 if self.connection else True

    def close(self):
        """
        Closes the connection.

        **NOTE:** This is a synchronous method.
        """
        if self.connection:
            self.connection.close()


def connect(*args, **kwargs):
    """
    Connection factory.
    See :py:meth:`momoko.Connection` for documentation about the parameters.

    Returns future that resolves to :py:meth:`momoko.Connection` object or raises exception.
    """
    return Connection(*args, **kwargs).connect()
