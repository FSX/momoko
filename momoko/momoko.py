#!/usr/bin/env python

__authors__ = ('Frank Smit <frank@61924.nl>',)
__version__ = '0.1.0'
__license__ = 'MIT'


import functools

import psycopg2
from tornado.ioloop import IOLoop, PeriodicCallback


class Pool(object):
    """A connection pool that manages PostgreSQL connections.
    """
    def __init__(self, min_conn, max_conn, cleanup_timeout, *args, **kwargs):
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

    def _new_conn(self, new_cursor_args={}):
        """Create a new connection. If `new_cursor_args` is provided a new
        cursor is created when the callback is executed.
        """
        if len(self._pool) > self.max_conn:
            raise PoolError('connection pool exausted')
        conn = psycopg2.connect(*self._args, **self._kwargs)
        add_conn = functools.partial(self._add_conn, conn)

        if new_cursor_args:
            new_cursor_args['connection'] = conn
            new_cursor = functools.partial(self._new_cursor, **new_cursor_args)
            Poller(conn, (add_conn, new_cursor)).start()
        else:
            Poller(conn, (add_conn,)).start()

    def _add_conn(self, conn):
        """Add a connection to the pool. This function is used by `_new_conn`
        as a callback to add the created connection to the pool.
        """
        self._pool.append(conn)

    def _new_cursor(self, function, func_args=(), callback=None, connection=None):
        """Create a new cursor. If there's no connection available, a new
        connection will be created and `_new_cursor` will be called again after
        the connection has been made.
        """
        if not connection:
            connection = self._get_free_conn()
            if not connection:
                new_cursor_args = {
                    'function': function,
                    'func_args': func_args,
                    'callback': callback
                }
                self._new_conn(new_cursor_args)
                return

        cursor = connection.cursor()
        getattr(cursor, function)(*func_args)

        # Callbacks from cursor fucntion always get the cursor back
        callback = functools.partial(callback, cursor)
        Poller(cursor.connection, (callback,)).start()

    def _get_free_conn(self):
        """Look for a free connection and return it. `None` is returned when no
        free connection can be found.
        """
        if self.closed:
            raise PoolError('connection pool is closed')
        for conn in self._pool:
            if not conn.isexecuting():
                return conn
        return None

    def _clean_pool(self):
        """Try to close the number of connections that exceeds the number in
        `min_conn`. This method loops throught the connections in `_pool` and
        if it finds a free connection it closes it.
        """
        if self.closed:
            raise PoolError('connection pool is closed')
        if len(self._pool) > self.min_conn:
            conns = len(self._pool) - self.min_conn
            indexes = []
            for i, conn in enumerate(self._pool):
                if not conn.isexecuting():
                    conn.close()
                    conns = conns - 1
                    indexes.append(i)
                    if conns == 0:
                        break
            for i in indexes:
                self._pool.pop(i)

    def execute(self, operation, parameters=(), callback=None):
        """http://initd.org/psycopg/docs/cursor.html#cursor.execute
        """
        self._new_cursor('execute', (operation, parameters), callback)

    def executemany(self, operation, parameters=None, callback=None):
        """http://initd.org/psycopg/docs/cursor.html#cursor.executemany
        """
        self._new_cursor('executemany', (operation, parameters), callback)

    def callproc(self, procname, parameters=None, callback=None):
        """http://initd.org/psycopg/docs/cursor.html#cursor.callproc
        """
        self._new_cursor('callproc', (procname, parameters), callback)

    def close(self):
        """Close all open connections.
        """
        if self.closed:
            raise PoolError('connection pool is closed')
        for conn in self._pool:
            if not conn.closed:
                conn.close()
        self._pool = []
        self.closed = True


class Poller(object):
    """A poller that polls the PostgreSQL connection and calls the callbacks
    when the connection state is `POLL_OK`.
    """
    def __init__(self, connection, callbacks=()):
        self._ioloop = IOLoop.instance()
        self._connection = connection
        self._callbacks = callbacks

    def start(self):
        """Start polling the connection.
        """
        self._update_handler()

    def _update_handler(self):
        state = self._connection.poll()
        if state == psycopg2.extensions.POLL_OK:
            for callback in self._callbacks:
                callback()
        elif state == psycopg2.extensions.POLL_READ:
            self._ioloop.add_handler(self._connection.fileno(), self._io_callback, IOLoop.READ)
        elif state == psycopg2.extensions.POLL_WRITE:
            self._ioloop.add_handler(self._connection.fileno(), self._io_callback, IOLoop.WRITE)

    def _io_callback(self, *args):
        self._ioloop.remove_handler(self._connection.fileno())
        self._update_handler()


class PoolError(Exception):
    pass
