#!/usr/bin/env python

__authors__ = ('Frank Smit <frank@61924.nl>',)
__version__ = '0.2.0'
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


class QueryChain(object):

    def __init__(self, db, chain):
        self._db = db
        self._args = None
        self._chain = chain
        self._chain.reverse()

    def _collect(self, *args, **kwargs):
        if not self._chain:
            return
        link = self._chain.pop()
        if callable(link):
            results = link(*args, **kwargs)
            if type(results) is type([]) or type(results) is type(()):
                self._collect(*results)
            elif type(results) is type({}):
                self._collect(**results)
            else:
                self._collect(results)
        else:
            if len(link) < 2:
                link.append(args)
            elif type(link[1]) is type([]):
                link[1].extend(args)
            self._db.execute(*link, callback=self._collect)

    def run(self):
        self._collect(None)


class BatchQuery(object):
    """`BatchQuery` is a helper class to run a batch of query all at once. It
    will call the final callback once all the queries are executed and all the
    cursors will be passed to that callback. The downside is that it will also
    open the same amount of database connections.

    The first parameter is a dictionary of queries. The key is used to identify
    the query and the value is a list with the SQL and a tuple of parameters.
    The rules for passing parameters to SQL queries in Psycopg apply to this [1].

    The second parameter is the callback that is called after all the queries
    are executed. This must be a callable that accepts atleast one argument
    with that contains a dictionary with all the cursors.

     [1]:http://initd.org/psycopg/docs/usage.html#passing-parameters-to-sql-queries
    """
    def __init__(self, queries, callback):
        self._callback = callback
        self._queries = {}
        self._args = {}
        self._size = len(queries)

        for key, query in queries.items():
            if len(query) < 2:
                query.append(())
            query.append(functools.partial(self._collect, key))
            self._queries[key] = query

    def _collect(self, key, cursor):
        """This function is called after each query is executed. It collects
        all the cursors in a dictionary with the related keys. Once a all
        queries are executed the final callback will be executed and the
        collected cursors will be passed on to it.
        """
        self._size = self._size - 1
        self._args[key] = cursor
        if not self._size:
            self._callback(self._args)

    def batch(self):
        """Return the dictionary with queries.
        """
        return self._queries


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
