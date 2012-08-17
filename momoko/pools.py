# -*- coding: utf-8 -*-
"""
    momoko.pools
    ~~~~~~~~~~~~

    This module contains all the connection pools.

    :copyright: (c) 2011 by Frank Smit.
    :license: MIT, see LICENSE for more details.
"""

import logging
from functools import partial
from contextlib import contextmanager
import time

import psycopg2
from psycopg2 import DatabaseError, InterfaceError
from psycopg2.extensions import STATUS_READY
from tornado.ioloop import IOLoop, PeriodicCallback


class BlockingPool(object):
    """A connection pool that manages blocking PostgreSQL connections
    and cursors.

    :param min_conn: The minimum amount of connections that is created when a
                     connection pool is created.
    :param max_conn: The maximum amount of connections the connection pool can
                     have. If the amount of connections exceeds the limit a
                     ``PoolError`` exception is raised.
    :param cleanup_timeout: Time in seconds between pool cleanups. Connections
                            will be closed until there are ``min_conn`` left.
    :param database: The database name
    :param user: User name used to authenticate
    :param password: Password used to authenticate
    :param connection_factory: Using the connection_factory parameter a different
                               class or connections factory can be specified. It
                               should be a callable object taking a dsn argument.
    """
    def __init__(self, min_conn=1, max_conn=20, cleanup_timeout=10,
                 *args, **kwargs):
        self.min_conn = min_conn
        self.max_conn = max_conn
        self.closed = False

        self._args = args
        self._kwargs = kwargs

        self._pool = []

        for i in range(self.min_conn):
            self._new_conn()

        # Create a periodic callback that tries to close inactive connections
        if cleanup_timeout > 0:
            self._cleaner = PeriodicCallback(self._clean_pool,
                cleanup_timeout * 1000)
            self._cleaner.start()

    def _new_conn(self):
        """Create a new connection.
        """
        if len(self._pool) > self.max_conn:
            raise PoolError('connection pool exhausted')
        conn = psycopg2.connect(*self._args, **self._kwargs)
        self._pool.append(conn)

        return conn

    def _get_free_conn(self):
        """Look for a free connection and return it.

        `None` is returned when no free connection can be found.
        """
        if self.closed:
            raise PoolError('connection pool is closed')
        for conn in self._pool:
            if conn.status == STATUS_READY:
                return conn
        return None

    def get_connection(self):
        """Get a connection from the pool.

        If there's no free connection available, a new connection will be created.
        """
        connection = self._get_free_conn()
        if not connection:
            connection = self._new_conn()

        return connection

    def _clean_pool(self):
        """Close a number of inactive connections when the number of connections
        in the pool exceeds the number in `min_conn`.
        """
        if self.closed:
            raise PoolError('connection pool is closed')
        if len(self._pool) > self.min_conn:
            conns = len(self._pool) - self.min_conn
            for conn in self._pool[:]:
                if conn.status == STATUS_READY:
                    conn.close()
                    conns -= 1
                    self._pool.remove(conn)
                    if conns == 0:
                        break

    def close(self):
        """Close all open connections in the pool.
        """
        if self.closed:
            raise PoolError('connection pool is closed')
        for conn in self._pool:
            if not conn.closed:
                conn.close()
        self._cleaner.stop()
        self._pool = []
        self.closed = True


class AsyncPool(object):
    """A connection pool that manages asynchronous PostgreSQL connections
    and cursors.

    :param min_conn: The minimum amount of connections that is created when a
                     connection pool is created.
    :param max_conn: The maximum amount of connections the connection pool can
                     have. If the amount of connections exceeds the limit a
                     ``PoolError`` exception is raised.
    :param cleanup_timeout: Time in seconds between pool cleanups. Connections
                            will be closed until there are ``min_conn`` left.
    :param ioloop: An instance of Tornado's IOLoop.
    :param host: The database host address (defaults to UNIX socket if not provided)
    :param port: The database host port (defaults to 5432 if not provided)
    :param database: The database name
    :param user: User name used to authenticate
    :param password: Password used to authenticate
    :param connection_factory: Using the connection_factory parameter a different
                               class or connections factory can be specified. It
                               should be a callable object taking a dsn argument.
    """
    def __init__(self, min_conn=1, max_conn=20, cleanup_timeout=10,
                 ioloop=None, *args, **kwargs):
        self.min_conn = min_conn
        self.max_conn = max_conn
        self.closed = False
        self._ioloop = ioloop or IOLoop.instance()
        self._args = args
        self._kwargs = kwargs
        self._last_reconnect = 0 # prevents DOS'sing the DB server with connect requests

        self._pool = []

        for i in range(self.min_conn):
            self._new_conn()
        self._last_reconnect = time.time()

        # Create a periodic callback that tries to close inactive connections
        if cleanup_timeout > 0:
            self._cleaner = PeriodicCallback(self._clean_pool,
                cleanup_timeout * 1000)
            self._cleaner.start()

    def _new_conn(self, callback, callback_args=[]):
        """Create a new connection.

        If `new_cursor_args` is provided a new cursor is created when the
        callback is executed.

        :param new_cursor_args: Arguments (dictionary) for a new cursor.
        """
        if len(self._pool) > self.max_conn:
            self._clean_pool()
        if len(self._pool) > self.max_conn:
            raise PoolError('connection pool exhausted')
        timeout = self._last_reconnect + .1
        timenow = time.time()
        if timenow > timeout or len(self._pool) < self.min_conn:
            self._last_reconnect = timenow
            conn = AsyncConnection(self._ioloop)
            callbacks = [partial(self._pool.append, conn)] # add new connection to the pool
            if callback_args:
                callback_args.append(conn)
                callbacks.append(partial(callback, *callback_args))

            conn.open(callbacks, *self._args, **self._kwargs)
        else:
            self._ioloop.add_timeout(timeout,partial(self._new_conn,callback,callback_args))


    def new_cursor(self, function, function_args=(), callback=None, cursor_kwargs={},
        connection=None):
        """Create a new cursor.

        If there's no connection available, a new connection will be created and
        `new_cursor` will be called again after the connection has been made.

        :param function: ``execute``, ``executemany`` or ``callproc``.
        :param function_args: A tuple with the arguments for the specified function.
        :param callback: A callable that is executed once the operation is done.
        :param cursor_kwargs: A dictionary with Psycopg's `connection.cursor`_ arguments.
        :param connection: An ``AsyncConnection`` connection. Optional.

        .. _connection.cursor: http://initd.org/psycopg/docs/connection.html#connection.cursor
        """
        if connection is None:
            self._new_conn(callback=self.new_cursor, callback_args=[function, function_args, callback, cursor_kwargs])
        else:
            try:
                connection.cursor(function, function_args, callback, cursor_kwargs)
            except (DatabaseError, InterfaceError):  # Recover from lost connection
                logging.warning('Requested connection was closed')
                self._pool.remove(connection)
                connection = self._get_free_conn()
                if connection is None:
                    self._new_conn([function, function_args, callback, cursor_kwargs])
                else:
                    self.new_cursor(function, function_args, callback, cursor_kwargs, connection)

    def _get_free_conn(self):
        """Look for a free connection and return it.

        `None` is returned when no free connection can be found.
        """
        if self.closed:
            raise PoolError('connection pool is closed')
        for conn in self._pool:
            if not conn.isexecuting():
                return conn
        return None

    def _clean_pool(self):
        """Close a number of inactive connections when the number of connections
        in the pool exceeds the number in `min_conn`.
        """
        if self.closed:
            raise PoolError('connection pool is closed')
        if len(self._pool) > self.min_conn:
            conns = len(self._pool) - self.min_conn
            for conn in self._pool[:]:
                if not conn.isexecuting():
                    conn.close()
                    conns -= 1
                    self._pool.remove(conn)
                    if conns == 0:
                        break

    def close(self):
        """Close all open connections in the pool.
        """
        if self.closed:
            raise PoolError('connection pool is closed')
        for conn in self._pool:
            if not conn.closed:
                conn.close()
        self._cleaner.stop()
        self._pool = []
        self.closed = True


class PoolError(Exception):
    pass


class AsyncConnection(object):
    """An asynchronous connection object.

    :param ioloop: An instance of Tornado's IOLoop.
    """
    def __init__(self, ioloop):
        self._conn = None
        self._fileno = -1
        self._ioloop = ioloop
        self._callbacks = []

    def open(self, callbacks, *args, **kwargs):
        """Open the connection to the database,

        :param host: The database host address (defaults to UNIX socket if not provided)
        :param port: The database host port (defaults to 5432 if not provided)
        :param database: The database name
        :param user: User name used to authenticate
        :param password: Password used to authenticate
        :param connection_factory: Using the connection_factory parameter a different
                                   class or connections factory can be specified. It
                                   should be a callable object taking a dsn argument.
        """
        self._conn = psycopg2.connect(async=1, *args, **kwargs)
        self._fileno = self._conn.fileno()
        self._callbacks = callbacks

        # Connection state should be 2 (write)
        self._ioloop.add_handler(self._fileno, self._io_callback, IOLoop.WRITE)

    def cursor(self, function, function_args, callback, cursor_kwargs={}):
        """Get a cursor and execute the requested function

        :param function: ``execute``, ``executemany`` or ``callproc``.
        :param function_args: A tuple with the arguments for the specified function.
        :param callback: A callable that is executed once the operation is done.
        :param cursor_kwargs: A dictionary with Psycopg's `connection.cursor`_ arguments.

        .. _connection.cursor: http://initd.org/psycopg/docs/connection.html#connection.cursor
        """
        cursor = self._conn.cursor(**cursor_kwargs)
        getattr(cursor, function)(*function_args)
        self._callbacks = [partial(callback, cursor)]

        # Connection state should be 1 (write)
        self._ioloop.update_handler(self._fileno, IOLoop.READ)

    def _io_callback(self, fd, events):
        state = self._conn.poll()

        if state == psycopg2.extensions.POLL_OK:
            for callback in self._callbacks:
                callback()
        elif state == psycopg2.extensions.POLL_READ:
            self._ioloop.update_handler(self._fileno, IOLoop.READ)
        elif state == psycopg2.extensions.POLL_WRITE:
            self._ioloop.update_handler(self._fileno, IOLoop.WRITE)

    def close(self):
        """Close connection.
        """
        self._ioloop.remove_handler(self._fileno)
        return self._conn.close()

    @property
    def closed(self):
        """Read-only attribute reporting whether the database connection is
        open (0) or closed (1).
        """
        return self._conn.closed

    def isexecuting(self):
        """Return True if the connection is executing an asynchronous operation.
        """
        return self._conn.isexecuting()
