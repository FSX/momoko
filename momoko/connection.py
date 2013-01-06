# -*- coding: utf-8 -*-
"""
momoko.connection
=================

Connection handling.

Copyright 2011-2012 by Frank Smit.
MIT, see LICENSE for more details.
"""

from functools import partial
from contextlib import contextmanager
from collections import deque, defaultdict

import psycopg2
from psycopg2.extensions import (connection as base_connection, cursor as base_cursor,
    POLL_OK, POLL_READ, POLL_WRITE, POLL_ERROR, TRANSACTION_STATUS_IDLE)

from tornado import gen
from tornado.ioloop import IOLoop, PeriodicCallback

from .utils import Op
from .exceptions import PoolError


# The dummy callback is used to keep the asynchronous cursor alive in case no
# callback has been specified. This will prevent the cursor from being garbage
# collected once, for example, ``Pool.execute`` has finished.
def _dummy_callback(cursor, error):
    pass


class Pool:
    """
    Asynchronous connection pool.

    The pool manages database connections and passes operations to connections.

    See :py:class:`momoko.Connection` for documentation about the ``dsn`` and
    ``connection_factory`` parameters. These are used by the connection pool when
    a new connection is created.

    :param integer minconn: Amount of connections created upon initialization. Defaults to ``1``.
    :param integer maxconn: Maximum amount of connections allowed by the pool. Defaults to ``5``.
    :param integer cleanup_timeout:
        Time in seconds between pool cleanups. Unused connections are closed and
        removed from the pool until only ``minconn`` are left. When an integer
        below ``1``, like ``-1`` is used the pool cleaner will be disabled.
        Defaults to ``10``.
    :param callable callback:
        A callable that's called after all the connections are created. Defaults to ``None``.
    :param ioloop: An instance of Tornado's IOLoop. Defaults to ``None``.
    """
    def __init__(self,
        dsn,
        connection_factory=None,
        minconn=1,
        maxconn=5,
        cleanup_timeout=10,
        callback=None,
        ioloop=None
    ):
        self.dsn = dsn
        self.minconn = minconn
        self.maxconn = maxconn
        self.closed = False
        self.connection_factory = connection_factory

        self._ioloop = ioloop or IOLoop.instance()
        self._pool = []

        # Create connections
        if callback:
            self._after_connect = self.minconn

            def after_connect(_):
                self._after_connect -= 1
                if self._after_connect == 0:
                    callback()

            for i in range(self.minconn):
                self.new(after_connect)
        else:
            for i in range(self.minconn):
                self.new()

        # Create a periodic callback that tries to close inactive connections
        self._cleaner = None
        if cleanup_timeout > 0:
            self._cleaner = PeriodicCallback(self._clean_pool,
                cleanup_timeout * 1000)
            self._cleaner.start()

    def new(self, callback=None):
        """
        Create a new connection and add it to the pool.

        :param callable callback:
            A callable that's called after the connection is created. It accepts
            one paramater: an instance of :py:class:`momoko.Connection`. Defaults to ``None``.
        """
        if len(self._pool) > self.maxconn:
            raise PoolError('connection pool exausted')

        def multi_callback(connection, error):
            if error:
                raise error
            if callback:
                callback(connection)
            self._pool.append(connection)

        Connection(self.dsn, self.connection_factory,
            multi_callback, self._ioloop)

    def _get_connection(self):
        for connection in self._pool:
            if not connection.busy():
                return connection

    def _clean_pool(self):
        if self.closed:
            raise PoolError('connection pool is closed')
        if len(self._pool) > self.minconn:
            connection_count = len(self._pool) - self.minconn
            for connection in self._pool[:]:
                if not connection.busy():
                    connection.close()
                    connection_count -= 1
                    self._pool.remove(connection)
                    if not connection_count:
                        break

    def transaction(self,
        statements,
        cursor_factory=None,
        callback=None,
        connection=None
    ):
        """
        Run a sequence of SQL queries in a database transaction.

        See :py:meth:`momoko.Connection.transaction` for documentation about the
        parameters. The ``connection`` parameter is for internal use.
        """
        connection = connection or self._get_connection()
        if not connection:
            return self.new(lambda connection: self.transaction(
                statements, cursor_factory, callback, connection))

        connection.transaction(statements, cursor_factory, callback)

    def execute(self,
        operation,
        parameters=(),
        cursor_factory=None,
        callback=None,
        connection=None
    ):
        """
        Prepare and execute a database operation (query or command).

        See :py:meth:`momoko.Connection.execute` for documentation about the
        parameters. The ``connection`` parameter is for internal use.
        """
        connection = connection or self._get_connection()
        if not connection:
            return self.new(lambda connection: self.execute(operation,
                parameters, cursor_factory, callback, connection))

        connection.execute(operation, parameters, cursor_factory, callback)

    def callproc(self,
        procname,
        parameters=(),
        cursor_factory=None,
        callback=None,
        connection=None
    ):
        """
        Call a stored database procedure with the given name.

        See :py:meth:`momoko.Connection.callproc` for documentation about the
        parameters. The ``connection`` parameter is for internal use.
        """
        connection = connection or self._get_connection()
        if not connection:
            return self.new(lambda connection: self.callproc(procname,
                parameters, cursor_factory, callback, connection))

        connection.callproc(procname, parameters, cursor_factory, callback)

    def mogrify(self,
        operation,
        parameters=(),
        callback=None,
        connection=None
    ):
        """
        Return a query string after arguments binding.

        See :py:meth:`momoko.Connection.mogrify` for documentation about the
        parameters. The ``connection`` parameter is for internal use.
        """
        connection = connection or self._get_connection()
        if not connection:
            return self.new(lambda connection: self.mogrify(operation,
                parameters, callback, connection))

        connection.mogrify(operation, parameters, callback)

    def close(self):
        """
        Close the connection pool.
        """
        if self.closed:
            raise PoolError('connection pool is already closed')

        for connection in self._pool:
            if not connection.closed:
                connection.close()

        if self._cleaner:
            self._cleaner.stop()
        self._pool = []
        self.closed = True


class Connection:
    """
    Create an asynchronous connection.

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

    .. _Data Source Name: http://en.wikipedia.org/wiki/Data_Source_Name
    .. _parameters: http://www.postgresql.org/docs/current/static/libpq-connect.html#LIBPQ-PQCONNECTDBPARAMS
    .. _psycopg2.extensions.connection: http://initd.org/psycopg/docs/connection.html#connection
    .. _Connection and cursor factories: http://initd.org/psycopg/docs/advanced.html#subclassing-cursor
    """
    def __init__(self,
        dsn,
        connection_factory=None,
        callback=None,
        ioloop=None
    ):
        self.connection = psycopg2.connect(dsn, async=1,
            connection_factory=connection_factory or base_connection)
        self.fileno = self.connection.fileno()
        self._transaction_status = self.connection.get_transaction_status
        self.ioloop = ioloop or IOLoop.instance()

        if callback:
            self.callback = partial(callback, self)

        self.ioloop.add_handler(self.fileno, self.io_callback, IOLoop.WRITE)

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
                raise OperationalError('poll() returned {0}'.format(state))

    def execute(self,
        operation,
        parameters=(),
        cursor_factory=None,
        callback=None
    ):
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
        callback=None
    ):
        """
        Call a stored database procedure with the given name.

        The sequence of parameters must contain one entry for each argument that
        the procedure expects. The result of the call is returned as modified copy
        of the input sequence. Input parameters are left untouched, output and
        input/output parameters replaced with possibly new values.

        The procedure may also provide a result set as output. This must then be
        made available through the standard `fetch*()`_ methods.

        :param string operation: The name of the database procedure.
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
        callback=None
    ):
        """
        Run a sequence of SQL queries in a database transaction.

        :param tuple/list statements:
            List or tuple containing SQL queries with or without parameters. An item
            can be a string (SQL query without parameters) or a tuple/list with two items,
            an SQL query and a tuple/list wuth parameters. An example::

                (
                    'SELECT 1, 2, 3;',  # Without parameters
                    ('SELECT 4, %s, 6, %s;', (5, 7)),  # With parameters
                )

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
