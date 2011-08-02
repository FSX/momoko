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
        """Run a batch of queries all at once. This is a wrapper around
        ``BatchQuery``. See the documentation of ``BatchQuery`` for a more
        detailed description.

        :param queries: A dictionary with all the queries. The key is the name
                        of the query and the value is a list with a string
                        (the query) and a tuple (optional) with parameters.
        :param callback: The function that needs to be executed once all the
                         queries are finished.
        """
        return BatchQuery(self, queries, callback)

    def chain(self, links):
        """Run a chain of queries and callables in a certain order. This is a
        wrapper around ``QueryChain``. See the documentation of ``QueryChain``
        for a more detailed description.

        :param links: A list with all the links in the chain.
        """
        return QueryChain(self, links)

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
        """Call a stored database procedure with the given name. The sequence
        of parameters must contain one entry for each argument that the procedure
        expects. The result of the call is returned as modified copy of the input
        sequence. Input parameters are left untouched, output and input/output
        parameters replaced with possibly new values.

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
    def chain(self, links, callback=None):
        """The ``chain`` function executes a set of queries and functions in a
        certain order. All the cursors from the executes queries are returned.

        ``chain`` accepts a list/tuple with "links" (as in "links in a chain").
        A link can be a string, an SQL query without paramaters; a list with
        two elements, an SQL query and a tuple with parameters; or a callable
        that accepts one argument, the cursor of the previous link (if it was a
        query) or the results of a callable.

        A cursor is not included in the returned result if it's been used in a
        callable. That means if link one is a query and link two is a callable,
        the cursor from link 1 will be pass to link 2 and will not be included
        in the returned result.

        The data that is returned by a callable is not included in the returned
        results. It will be passed to the next link. If the next link is a query
        the data can be used as query parameters if that query does not have
        paramaters.

        :param links: A list with all the links in the chain.
        """
        cursors = []
        results = None
        for link in links:
            if isinstance(link, collections.Callable):
                results = link(cursors.pop())
            else:
                if isinstance(link, str):
                    cursor = yield self.execute(link)
                else:
                    if len(link) < 2 and results:
                        link.append(results)
                    cursor = yield self.execute(*link)
                cursors.append(cursor)
                results = None
        callback(cursors)

    #TODO: Queries must be in a dictionary instead of a list
    @async
    @process
    def batch(self, queries, callback=None):
        """``batch`` runs a batch of queries all at once. This also creates a
        new connection for each query, since only one query can be executed per
        connection at the same time.

        :param queries: A list with queries. A list element can be a string
                        (only an SQL query) or a list/tuple with an SQL query
                        and a tuple with paramaters (or not).
        """
        def _exec_query(query, callback):
            if isinstance(query, str):
                cursor = yield self.execute(query)
            else:
                cursor = yield self.execute(*query)
            callback(cursor)
        cursors = yield list(map(async(process(_exec_query)), queries))
        callback(cursors)



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
        """Create a new connection. If `new_cursor_args` is provided a new
        cursor is created when the callback is executed.

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
        """Add a connection to the pool. This function is used by `_new_conn`
        as a callback to add the created connection to the pool.

        :param conn: A database connection.
        """
        self._pool.append(conn)

    def new_cursor(self, function, func_args=(), callback=None, connection=None):
        """Create a new cursor. If there's no connection available, a new
        connection will be created and `new_cursor` will be called again after
        the connection has been made.

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
    """A query chain that excutes a list of queries and callables in the
    specified order. Cursors will be passed to callables and the results of a
    callable will be passed to another callable or query. A chain is defined
    like this::

        QueryChain(db, [
            ['SELECT 42, 12, %s, 11;', (23,)],
            _after_first_query,
            _after_first_callable,
            ['SELECT 1, 2, 3, 4, 5;'],
            _before_last_query,
            ['SELECT %s, %s, %s, %s, %s;'],
            _last_callable
        ])

    A query is defined in a list with an SQL operation and a sequence of
    parameters.

    Once a query is executed and the next "link" is a callable, the callable
    will receive a cursor from the query that can be used to fetch the data.
    ``_after_first_query`` could look like this::

        def _after_first_query(cursor):
            results = cursor.fetchall()
            return {
                'p1': results[0][0],
                'p2': results[0][1],
                'p3': results[0][2],
                'p4': results[0][3]
            }

    ``_after_first_callable`` will be executed once ``_after_first_query`` is
    executed, but ``_after_first_query`` returned a dictionary so this dictionary
    will be passed to ``_after_first_callable``. So ``_after_first_callable``
    looks like this::

        def _after_first_callable(p1, p2, p3, p4):
            print '%s, %s, %s, %s<br>' % (p1, p2, p3, p4)

    Since ``_after_first_callable`` returns nothing the next query won't get
    any data from this callable. ``_before_last_query`` is more interesting.
    It receives a cursor from the previous query and does something with the
    fetched data and passes it to the next query. That data is used in the
    query. Here are the last two callables::

        def _before_last_query(cursor):
            results = cursor.fetchall()
            return [i*16 for i in results[0]]

        # ['SELECT %s, %s, %s, %s, %s;'] is executed here

        def _last_callable(cursor):
            print '%s' % cursor.fetchall()

    ``_last_callable`` prints the numbers that were modified in
    ``_before_last_query``.

    :param db: A ``Momoko`` instance.
    :param links: All the queries and callables that need to be executed.
    """
    def __init__(self, db, links):
        self._db = db
        self._args = None
        self._links = links
        self._links.reverse()
        self._collect(None)

    def _collect(self, *args, **kwargs):
        if not self._links:
            return
        link = self._links.pop()
        if isinstance(link, collections.Callable):
            results = link(*args, **kwargs)
            if isinstance(results, list) or isinstance(results, tuple):
                self._collect(*results)
            elif isinstance(results, dict):
                self._collect(**results)
            else:
                self._collect(results)
        else:
            if isinstance(link, str):
                link = [link]
            if len(link) < 2:
                link.append(args)
            elif isinstance(link[1], list):
                link[1].extend(args)
            self._db.execute(*link, callback=self._collect)


class BatchQuery(object):
    """The ``BatchQuery`` class executes a batch of queries all at the same time.
    Once all the queries are done a callback will be executed and all the cursors
    will be passed to the callback. The downside of this class is that for every
    query a new database connection will be created.

    A batch is defined like this::

        BatchQuery(db, {
            'query1': ['SELECT 42, 12, %s, 11;', (23,)],
            'query2': ['SELECT 1, 2, 3, 4, 5;'],
            'query3': ['SELECT 465767, 4567, 3454;']
        }, _batch_is_done)

    A query is defined in a list with an SQL operation and a sequence of
    parameters. The dictionary keys must be unique, because they are used to
    identify the cursors that are passed to the callback.

    The callback could look like this::

        def _batch_is_done(cursors):
            for key, cursor in cursors.items():
                print 'Query results: %s = %s<br>' % (key, cursor.fetchall())
            print 'Done'

    :param db: A ``Momoko`` instance.
    :param queries: A dictionary with queries.
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
