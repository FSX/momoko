# -*- coding: utf-8 -*-
"""
    momoko.client
    ~~~~~~~~~~~~~

    This module defines all core and helper classes.

    :copyright: (c) 2011 by Frank Smit.
    :license: MIT, see LICENSE for more details.
"""


import functools

import psycopg2
from tornado.ioloop import IOLoop, PeriodicCallback

from .adisp import async, process
import collections


class Client(object):
    """The Client class is a wrapper for ``Pool``, ``BatchQuery`` and
    ``QueryChain``. It also provides the ``execute`` and ``callproc`` functions.

    :param settings: A dictionary that is passed to the ``Pool`` object.
    """
    def __init__(self, settings):
        self._pool = Pool(**settings)

    def batch(self, queries, callback):
        """Run a batch of queries all at once.

        **Note:** Every query needs a free connection. So if three queries are
        are executed, three free connections are used.

        A dictionary with queries looks like this::

            {
                'query1': ['SELECT 42, 12, %s, %s;', (23, 56)],
                'query2': 'SELECT 1, 2, 3, 4, 5;',
                'query3': 'SELECT 465767, 4567, 3454;'
            }

        A query with paramaters is contained in a list: ``['some sql
        here %s, %s', ('and some', 'paramaters here')]``. A query
        without paramaters doesn't need to be in a list.

        :param queries: A dictionary with all the queries.
        :param callback: The function that needs to be executed once all the
                         queries are finished.
        :return: A dictionary with the same keys as the given queries with the
                 resulting cursors as values.
        """
        return BatchQuery(self, queries, callback)

    def chain(self, queries, callback):
        """Run a chain of queries in the given order.

        A list/tuple with queries looks like this::

            (
                ['SELECT 42, 12, %s, 11;', (23,)],
                'SELECT 1, 2, 3, 4, 5;'
            )

        A query with paramaters is contained in a list: ``['some sql
        here %s, %s', ('and some', 'paramaters here')]``. A query
        without paramaters doesn't need to be in a list.

        :param queries: A tuple or with all the queries.
        :param callback: The function that needs to be executed once all the
                         queries are finished.
        :return: A list with the resulting cursors.
        """
        return QueryChain(self, queries, callback)

    def execute(self, operation, parameters=(), callback=None):
        """Prepare and execute a database operation (query or command).

        Parameters may be provided as sequence or mapping and will be bound to
        variables in the operation. Variables are specified either with
        positional (``%s``) or named (``%(name)s``) placeholders. See Passing
        parameters to SQL queries `[1]`_ in the Psycopg2 documentation.

        .. _[1]: http://initd.org/psycopg/docs/usage.html#query-parameters

        :param operation: The database operation (an SQL query or command).
        :param parameters: A tuple, list or dictionary with parameters. This is
                           an empty tuple by default.
        :param callback: A callable that is executed once the operation is finised.
        """
        self._pool.new_cursor('execute', (operation, parameters), callback)

    def callproc(self, procname, parameters=None, callback=None):
        """Call a stored database procedure with the given name.

        The sequence of parameters must contain one entry for each argument that
        the procedure expects. The result of the call is returned as modified
        copy of the input sequence. Input parameters are left untouched, output
        and input/output parameters replaced with possibly new values.

        The procedure may also provide a result set as output. This must then
        be made available through the standard ``fetch*()`` methods.

        :param procname: The name of the procedure.
        :param parameters: A sequence with parameters. This is ``None`` by default.
        :param callback: A callable that is executed once the procedure is finised.
        """
        self._pool.new_cursor('callproc', (procname, parameters), callback)

    def close(self):
        """Close all connections in the connection pool.
        """
        self._pool.close()


class AdispClient(Client):
    """The AdispClient class is a wrapper for ``Pool`` and uses adisp to
    let the developer use the ``execute``, ``callproc``, ``chain`` and ``batch``
    functions in a blocking style. The ``chain`` and ``batch`` functions are
    slightly different than the two in ``Client``.

    :param settings: A dictionary that is passed to the ``Pool`` object.
    """

    execute = async(Client.execute)
    callproc = async(Client.callproc)

    @async
    @process
    def chain(self, queries, callback=None):
        """Run a chain of queries in the given order.

        A list/tuple with queries looks like this::

            (
                ['SELECT 42, 12, %s, 11;', (23,)],
                'SELECT 1, 2, 3, 4, 5;'
            )

        A query with paramaters is contained in a list: ``['some sql
        here %s, %s', ('and some', 'paramaters here')]``. A query
        without paramaters doesn't need to be in a list.

        :param queries: A tuple or with all the queries.
        :param callback: The function that needs to be executed once all the
                         queries are finished.
        :return: A list with the resulting cursors.
        """
        cursors = []
        for query in queries:
            if isinstance(query, str):
                cursor = yield self.execute(query)
            else:
                cursor = yield self.execute(*query)
            cursors.append(cursor)
        callback(cursors)

    @async
    @process
    def batch(self, queries, callback=None):
        """Run a batch of queries all at once.

        **Note:** Every query needs a free connection. So if three queries are
        are executed, three free connections are used.

        A dictionary with queries looks like this::

            {
                'query1': ['SELECT 42, 12, %s, %s;', (23, 56)],
                'query2': 'SELECT 1, 2, 3, 4, 5;',
                'query3': 'SELECT 465767, 4567, 3454;'
            }

        A query with paramaters is contained in a list: ``['some sql
        here %s, %s', ('and some', 'paramaters here')]``. A query
        without paramaters doesn't need to be in a list.

        :param queries: A dictionary with all the queries.
        :param callback: The function that needs to be executed once all the
                         queries are finished.
        :return: A dictionary with the same keys as the given queries with the
                 resulting cursors as values.
        """
        def _exec_query(query, callback):
            if isinstance(query[1], str):
                cursor = yield self.execute(query[1])
            else:
                cursor = yield self.execute(*query[1])
            callback((query[0], cursor))
        cursors = yield list(map(async(process(_exec_query)), queries.items()))
        callback(dict(cursors))



class Pool(object):
    """A connection pool that manages PostgreSQL connections and cursors.

    :param min_conn: The minimum amount of connections that is created when a
                     connection pool is created.
    :param max_conn: The maximum amount of connections the connection pool can
                     have. If the amount of connections exceeds the limit a
                     ``PoolError`` exception is raised.
    :param host: The database host address (defaults to UNIX socket if not provided)
    :param database: The database name
    :param user: User name used to authenticate
    :param password: Password used to authenticate
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
            Poller(conn, (add_conn, new_cursor)).start()
        else:
            Poller(conn, (add_conn,)).start()

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


class QueryChain(object):
    """Run a chain of queries in the given order.

    A list/tuple with queries looks like this::

        (
            ['SELECT 42, 12, %s, 11;', (23,)],
            'SELECT 1, 2, 3, 4, 5;'
        )

    A query with paramaters is contained in a list: ``['some sql
    here %s, %s', ('and some', 'paramaters here')]``. A query
    without paramaters doesn't need to be in a list.

    :param db: A ``momoko.Client`` or ``momoko.AdispClient`` instance.
    :param queries: A tuple or with all the queries.
    :param callback: The function that needs to be executed once all the
                     queries are finished.
    :return: A list with the resulting cursors is passed on to the callback.
    """
    def __init__(self, db, queries, callback):
        self._db = db
        self._cursors = []
        self._queries = list(queries)
        self._queries.reverse()
        self._callback = callback
        self._collect(None)

    def _collect(self, cursor):
        if cursor is not None:
            self._cursors.append(cursor)
        if not self._queries:
            self._callback(self._cursors)
            return
        query = self._queries.pop()
        if isinstance(query, str):
            query = [query]
        self._db.execute(*query, callback=self._collect)


class BatchQuery(object):
    """Run a batch of queries all at once.

    **Note:** Every query needs a free connection. So if three queries are
    are executed, three free connections are used.

    A dictionary with queries looks like this::

        {
            'query1': ['SELECT 42, 12, %s, %s;', (23, 56)],
            'query2': 'SELECT 1, 2, 3, 4, 5;',
            'query3': 'SELECT 465767, 4567, 3454;'
        }

    A query with paramaters is contained in a list: ``['some sql
    here %s, %s', ('and some', 'paramaters here')]``. A query
    without paramaters doesn't need to be in a list.

    :param db: A ``momoko.Client`` or ``momoko.AdispClient`` instance.
    :param queries: A dictionary with all the queries.
    :param callback: The function that needs to be executed once all the
                     queries are finished.
    :return: A dictionary with the same keys as the given queries with the
             resulting cursors as values is passed on to the callback.
    """
    def __init__(self, db, queries, callback):
        self._db = db
        self._callback = callback
        self._queries = {}
        self._args = {}
        self._size = len(queries)

        for key, query in list(queries.items()):
            if isinstance(query, str):
                query = [query, ()]
            query.append(functools.partial(self._collect, key))
            self._queries[key] = query

        for query in list(self._queries.values()):
            self._db.execute(*query)

    def _collect(self, key, cursor):
        self._size = self._size - 1
        self._args[key] = cursor
        if not self._size:
            self._callback(self._args)


class Poller(object):
    """A poller that polls the PostgreSQL connection and calls the callbacks
    when the connection state is ``POLL_OK``.

    :param connection: The connection that needs to be polled.
    :param callbacks: A tuple/list of callbacks.
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
            self._ioloop.add_handler(self._connection.fileno(),
                self._io_callback, IOLoop.READ)
        elif state == psycopg2.extensions.POLL_WRITE:
            self._ioloop.add_handler(self._connection.fileno(),
                self._io_callback, IOLoop.WRITE)

    def _io_callback(self, *args):
        self._ioloop.remove_handler(self._connection.fileno())
        self._update_handler()


class PoolError(Exception):
    pass
