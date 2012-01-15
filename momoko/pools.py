# -*- coding: utf-8 -*-
"""
    momoko.pools
    ~~~~~~~~~~~~

    This module contains all the connection pools.

    :copyright: (c) 2011 by Frank Smit.
    :license: MIT, see LICENSE for more details.
"""

import logging
import functools

import psycopg2
from psycopg2 import DatabaseError, InterfaceError
from psycopg2.extensions import STATUS_READY
from tornado.ioloop import IOLoop, PeriodicCallback

from .utils import Poller


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
            raise PoolError('connection pool exausted')
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
                    conns = conns - 1
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

        self._pool = []

        for i in range(self.min_conn):
            self._new_conn()

        # Create a periodic callback that tries to close inactive connections
        if cleanup_timeout > 0:
            self._cleaner = PeriodicCallback(self._clean_pool,
                cleanup_timeout * 1000)
            self._cleaner.start()

    def _new_conn(self, new_cursor_args={}):
        """Create a new connection.

        If `new_cursor_args` is provided a new cursor is created when the
        callback is executed.

        :param new_cursor_args: Arguments (dictionary) for a new cursor.
        """
        if len(self._pool) > self.max_conn:
            raise PoolError('connection pool exausted')
        conn = psycopg2.connect(async=1, *self._args, **self._kwargs)
        add_conn = functools.partial(self._add_conn, conn)

        if new_cursor_args:
            new_cursor_args['connection'] = conn
            new_cursor = functools.partial(self.new_cursor, **new_cursor_args)
            Poller(conn, (add_conn, new_cursor), ioloop=self._ioloop)
        else:
            Poller(conn, (add_conn,), ioloop=self._ioloop)

    def _add_conn(self, conn):
        """Add a connection to the pool.

        This function is used by `_new_conn` as a callback to add the created
        connection to the pool.

        :param conn: A database connection.
        """
        self._pool.append(conn)

    def new_cursor(self, function, func_args=(), callback=None, connection=None):
        """Create a new cursor.

        If there's no connection available, a new connection will be created and
        `new_cursor` will be called again after the connection has been made.

        :param function: ``execute``, ``executemany`` or ``callproc``.
        :param func_args: A tuple with the arguments for the specified function.
        :param callback: A callable that is executed once the operation is done.
        """
        if not connection:
            connection = self._get_free_conn()
            if not connection:
                self._new_conn({
                    'function': function,
                    'func_args': func_args,
                    'callback': callback
                })
                return

        try:
            cursor = connection.cursor()
            getattr(cursor, function)(*func_args)

            # Callbacks from cursor functions always get the cursor back
            if callback:
                Poller(cursor.connection, (functools.partial(callback, cursor),),
                    ioloop=self._ioloop)
        except (DatabaseError, InterfaceError):
            logging.warning('Requested connection was closed')
            self._pool.remove(connection)
            connection = self._get_free_conn()
            if not connection:
                self._new_conn({
                    'function': function,
                    'func_args': func_args,
                    'callback': callback
                })
            else:
                self.new_cursor(function, func_args, callback, connection)

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
                    conns = conns - 1
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
        self._pool = []
        self.closed = True


class PoolError(Exception):
    pass
