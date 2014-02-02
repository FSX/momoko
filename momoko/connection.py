# -*- coding: utf-8 -*-
"""
momoko.connection
=================

Connection handling.

Copyright 2011-2013 by Frank Smit.
MIT, see LICENSE for more details.
"""

from functools import partial
from collections import deque

import psycopg2
from psycopg2.extras import register_hstore as _psy_register_hstore
from psycopg2.extensions import (connection as base_connection, cursor as base_cursor,
    POLL_OK, POLL_READ, POLL_WRITE, POLL_ERROR, TRANSACTION_STATUS_IDLE)

from tornado import gen
from tornado.ioloop import IOLoop

from .exceptions import PoolError


# The dummy callback is used to keep the asynchronous cursor alive in case no
# callback has been specified. This will prevent the cursor from being garbage
# collected once, for example, ``Pool.execute`` has finished.
def _dummy_callback(cursor, error):
    pass


class Pool(object):
    """
    Asynchronous connection pool.

    The pool manages database connections and passes operations to connections.

    See :py:class:`momoko.Connection` for documentation about the ``dsn`` and
    ``connection_factory`` parameters. These are used by the connection pool when
    a new connection is created.

    :param integer size: Amount of connections created upon initialization. Defaults to ``1``.
    :param callable callback:
        A callable that's called after all the connections are created. Defaults to ``None``.
    :param ioloop: An instance of Tornado's IOLoop. Defaults to ``None``.
    """
    def __init__(self,
        dsn,
        connection_factory=None,
        size=1,
        callback=None,
        ioloop=None
    ):
        assert size > 0, 'The connection pool size must be a number above 0.'

        self.dsn = dsn
        self.size = size
        self.closed = False
        self.connection_factory = connection_factory

        self._ioloop = ioloop or IOLoop.instance()
        self._pool = []

        # Create connections
        def after_pool_creation(n, connection):
            if n == self.size-1:
                if callback:
                    callback()

        for i in range(self.size):
            self._new(partial(after_pool_creation, i))

    def _new(self, callback=None):
        def multi_callback(connection, error):
            if error:
                raise error
            self._pool.append(connection)
            if callback:
                callback(connection)

        Connection(self.dsn, self.connection_factory,
            multi_callback, self._ioloop)

    def _get_connection(self):
        for connection in self._pool:
            if not connection.busy():
                return connection

    def transaction(self,
        statements,
        cursor_factory=None,
        callback=None
    ):
        """
        Run a sequence of SQL queries in a database transaction.

        See :py:meth:`momoko.Connection.transaction` for documentation about the
        parameters.
        """
        connection = self._get_connection()
        if not connection:
            return self._ioloop.add_callback(partial(self.transaction,
                statements, cursor_factory, callback))

        connection.transaction(statements, cursor_factory, callback)

    def execute(self,
        operation,
        parameters=(),
        cursor_factory=None,
        callback=None
    ):
        """
        Prepare and execute a database operation (query or command).

        See :py:meth:`momoko.Connection.execute` for documentation about the
        parameters.
        """
        connection = self._get_connection()
        if not connection:
            return self._ioloop.add_callback(partial(self.execute,
                operation, parameters, cursor_factory, callback))

        connection.execute(operation, parameters, cursor_factory, callback)

    def callproc(self,
        procname,
        parameters=(),
        cursor_factory=None,
        callback=None
    ):
        """
        Call a stored database procedure with the given name.

        See :py:meth:`momoko.Connection.callproc` for documentation about the
        parameters.
        """
        connection = self._get_connection()
        if not connection:
            return self._ioloop.add_callback(partial(self.callproc,
                procname, parameters, cursor_factory, callback))

        connection.callproc(procname, parameters, cursor_factory, callback)

    def mogrify(self,
        operation,
        parameters=(),
        callback=None
    ):
        """
        Return a query string after arguments binding.

        See :py:meth:`momoko.Connection.mogrify` for documentation about the
        parameters.
        """
        self._pool[0].mogrify(operation, parameters, callback)

    def register_hstore(self, unicode=False, callback=None):
        """
        Register adapter and typecaster for ``dict-hstore`` conversions.

        See :py:meth:`momoko.Connection.register_hstore` for documentation about
        the parameters. This method has no ``globally`` parameter, because it
        already registers hstore to all the connections in the pool.
        """
        connection = self._get_connection()
        if not connection:
            return self._ioloop.add_callback(
                partial(self.register_hstore, unicode, callback))

        connection.register_hstore(True, unicode, callback)

    def close(self):
        """
        Close the connection pool.
        """
        if self.closed:
            raise PoolError('connection pool is already closed')

        for connection in self._pool:
            if not connection.closed:
                connection.close()

        self._pool = []
        self.closed = True


class Connection(object):
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
                raise psycopg2.OperationalError('poll() returned {0}'.format(state))

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

        :param string procname: The name of the database procedure.
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
            an SQL query and a tuple/list wuth parameters.

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

        .. _documentation: http://initd.org/psycopg/docs/extras.html#hstore-data-type
        """
        def _hstore_callback(cursor, error):
            oid, array_oid = cursor.fetchone()
            _psy_register_hstore(None, globally, unicode, oid, array_oid)

            if callback:
                callback(None, error)

        self.execute(
            "SELECT 'hstore'::regtype::oid, 'hstore[]'::regtype::oid",
            callback=_hstore_callback)

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
