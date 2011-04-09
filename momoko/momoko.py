# -*- coding: utf-8 -*-
"""
    momoko.momoko
    ~~~~~~~~~~~~~

    This module defines all core and helper classes.

    :copyright: (c) 2011 by Frank Smit.
    :license: MIT, see LICENSE for more details.
"""

__authors__ = ('Frank Smit <frank@61924.nl>',)
__version__ = '0.2.0'
__license__ = 'MIT'


import functools

import psycopg2
from tornado.ioloop import IOLoop, PeriodicCallback


class Momoko(object):
    """The Momoko class is a wrapper for ``Pool``, ``BatchQuery`` and
    ``QueryChain``. It also provides the ``execute``, ``executemany`` and
    ``callproc`` functions.

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

    def executemany(self, operation, seq_of_parameters=None, callback=None):
        """Prepare a database operation (query or command) and then execute it
        against all parameter tuples or mappings found in the sequence
        ``seq_of_parameters``.

        The function is mostly useful for commands that update the database:
        any result set returned by the query is discarded.

        Parameters are bounded to the query using the same rules described in
        the ``execute()`` method.

        :param operation: The database operation (an SQL query or command).
        :param parameters: A sequence with parameters.
        :param callback: A callable that is executed once the operation is finised.
        """
        self._pool.new_cursor('executemany', (operation, seq_of_parameters), callback)

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


class Pool(object):
    """A connection pool that manages PostgreSQL connections and cursors.

    :param min_conn: The minimum amount of connections that is created when a
                     connection pool is created.
    :param max_conn: The maximum amount of connections the connection pool can
                     have. If the amount of connections exceeds the limit a
                     ``PoolError`` exception is raised.
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
    """A query chain.
    """
    def __init__(self, db, links):
        self._db = db
        self._args = None
        self._links = links
        self._links.reverse()

    def _collect(self, *args, **kwargs):
        if not self._links:
            return
        link = self._links.pop()
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
        """Run the query chain.
        """
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
    def __init__(self, db, queries, callback):
        self._db = db
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

    def run(self):
        """Run the batch with queries.
        """
        for query in self._queries.values():
            self._db.execute(*query)


class Poller(object):
    """A poller that polls the PostgreSQL connection and calls the callbacks
    when the connection state is `POLL_OK`.

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
            self._ioloop.add_handler(self._connection.fileno(), self._io_callback, IOLoop.READ)
        elif state == psycopg2.extensions.POLL_WRITE:
            self._ioloop.add_handler(self._connection.fileno(), self._io_callback, IOLoop.WRITE)

    def _io_callback(self, *args):
        self._ioloop.remove_handler(self._connection.fileno())
        self._update_handler()


class PoolError(Exception):
    pass
