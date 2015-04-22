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

from functools import partial
from collections import deque
import datetime
from functools import wraps
from contextlib import contextmanager

import psycopg2
from psycopg2.extras import register_hstore as _psy_register_hstore
from psycopg2.extensions import POLL_OK, POLL_READ, POLL_WRITE, POLL_ERROR

from tornado import gen
from tornado.ioloop import IOLoop
from tornado.concurrent import chain_future, Future

from .exceptions import PoolError, PartiallyConnected

from .utils import log


# The dummy callback is used to keep the asynchronous cursor alive in case no
# callback has been specified. This will prevent the cursor from being garbage
# collected once, for example, ``Pool.execute`` has finished.
# Symptom: you'll get
#     InterfaceError: the asynchronous cursor has disappeared
# exceptions
def _dummy_callback(cursor, error):
    pass

# FIXME: use future.set_exc_info to preserve exceptions in Python 2. Think how to test it.

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
    FIXME: Doc for future that may be passed in and invoked with self when connection is done
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
                 ioloop=None,
                 raise_connect_errors=True,
                 reconnect_interval=500,
                 setsession=(),
                 future=None):
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
                future.set_result(self)

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
            **NOTE:** This is a synchronous function
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

        if not self.waiting_queue:
            self.free.add(conn)
            return

        self.busy.add(conn)
        future = self.waiting_queue.pop()
        future.set_result(conn)

    def add_dead(self, conn):
        self.pending.discard(conn)
        self.dead.add(conn)

    def acquire(self):
        """Occupy free connection"""
        future = Future()
        if self.free:
            conn = self.free.pop()
            self.busy.add(conn)
            future.set_result(conn)
            return future
        elif self.busy:
            self.waiting_queue.append_left(future)
            return future
        else:
            return None

    def release(self, conn):
        assert conn in self.busy, "Tried to release non-busy connection"
        if conn.closed:
            self.busy.remove(conn)
            self.dead.add(conn)
            return

        self.busy.remove(conn)
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

        #FIXME: address:
        If some connection failed to connected, raises PartiallyConnected
        if self.raise_connect_errors is true

        FIXME: add rst referenceses above
        """
        future = Future()

        def on_connect(fut):
            if self.conns.pending:
                return
            # all connection attemts are complete
            if self.conns.dead and self.raise_connect_errors:
                ecp = PartiallyConnected("%s connection(s) failed to connect" % len(self.conns.dead))
                future.set_exception(ecp)
            else:
                future.set_result(self)

        for i in range(self.size):
            self.ioloop.add_future(self._new_connection(), on_connect)

        return future

    def getconn(self, ping=True):  # FIXME: fix ping
        """
        Acquire connection from the pool.

        You can then use this connection for subsequest queries.
        Just use ``connection.execute`` instead of ``Pool.execute``.

        Make sure to return connection to the pool by calling :py:meth:`momoko.Pool.putconn`,
        otherwise the connection will remain forever-busy and you'll starvate your pool quickly.

        Returns future that resolves to the acquired connection object.

        :param boolean ping:
            Whether to ping the connection before returning it by executing :py:meth:`momoko.Pool.ping`.

        FIXME: How make sure that returned connection is not dead?
               And what happens when pinging dead connection?
        """
        # FIXME: ping dies on dead connections
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
            if not connection._keep:
                self.putconn(connection)

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
        return self._operate(Connection.register_hstore, args, kwargs)

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

    def _operate(self, method, args, kwargs, async=True):
        future = Future()

        retry = []

        def when_avaialble(fut):
            if retry:
                retry[0]._keep = False

            try:
                conn = fut.result()
            except psycopg2.Error as error:
                future.set_exc_info(sys.exc_info())
                if retry:
                    self.putconn(retry)
                return

            with self.manage(conn):
                try:
                    future_or_result = method(conn, *args, **kwargs)
                except psycopg2.Error as error:
                    if conn.closed:
                        if not retry:
                            conn._keep = True  # hint to self.manage above not to release this connection
                            retry.append(conn)
                            self.ioloop.add_future(conn.connect(), when_avaialble)
                        else:
                            future.set_exception(self._no_conn_availble_error)
                    else:
                        future.set_exc_info(sys.exc_info())
                    return

                if not async:
                    future.set_result(future_or_result)
                    return

                chain_future(future_or_result, future)

        # Disabling ping - we'll retry dead connections ourself
        self.ioloop.add_future(self.getconn(ping=False), when_avaialble)
        return future

    def _reanimate(self):
        assert not self.conns.dead, "BUG: dont' call reanimate when there is no one to reanimate"

        future = Future()

        if self.ioloop.time() - self._last_connect_time < self.reconnect_interval:
            future.set_result(None)
            return future

        def on_connect(fut):
            if not self.conns.pending:
                future.set_result(None)

        while self.conns.dead:
            conn = self.conns.dead.pop()
            self.ioloop.add_future(self._connect_one(conn), on_connect)

        self._last_connect_time = self.ioloop.time()
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
                conn = fut.result()
            except psycopg2.Error as error:
                self.conns.add_dead(conn)
            else:
                self.conns.add_free(conn)
                self.server_version = conn.server_version
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

            self.ioloop.add_future(conn.ping(), on_ping_done)

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
        log.info("Opening new database connection")

        self.dsn = dsn
        self.connection_factory = connection_factory
        self.cursor_factory = cursor_factory
        self.ioloop = ioloop or IOLoop.instance()
        self.setsession = setsession

        # For internal use - prevents Pool.manage to return this connection
        # back to the pool
        self._keep = False

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
            future.set_exception(error)
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
        self.server_version = self.connection.server_version

    def _io_callback(self, future, result, fd=None, events=None):
        try:
            state = self.connection.poll()
        except (psycopg2.Warning, psycopg2.Error) as error:
            self.ioloop.remove_handler(self.fileno)
            future.set_exception(error)
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

    def _ping_callback(self, callback, cursor, error):
        if not error:
            cursor.fetchall()
        return callback(self, error)

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
                    transaction_future.set_exception(error)
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
            except Exception, rb_error:
                log.warn("Failed to ROLLBACK transaction %s", rb_error)
            transaction_future.set_exception(error)
        self.ioloop.add_future(self.execute("ROLLBACK;"), rollback_callback)

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

        Returns future that resolves to ``None``.

        .. _documentation: http://initd.org/psycopg/docs/extras.html#hstore-data-type
        """
        future = Future()

        def hstore_callback(fut):
            try:
                cursor = fut.result()
            except Exception as error:
                future.set_exception(error)
                return

            oid, array_oid = cursor.fetchone()
            _psy_register_hstore(None, globally, unicode, oid, array_oid)
            future.set_result(None)

        self.ioloop.add_future(self.execute(
            "SELECT 'hstore'::regtype::oid, 'hstore[]'::regtype::oid",
        ), hstore_callback)

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
    See :py:meth:`momoko.Connection` for documentation about the
    Returns future that resolves to :py:meth:`momoko.Connection` object or raises exception
    """
    return Connection(*args, **kwargs).connect()
