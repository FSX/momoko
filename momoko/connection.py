# -*- coding: utf-8 -*-
"""
momoko.connection
=================

Connection handling.

Copyright 2011-2012 by Frank Smit.
MIT, see LICENSE for more details.
"""

# Information
#
# - http://initd.org/psycopg/articles/2010/12/01/postgresql-notifications-psycopg2-eventlet/
# - http://initd.org/psycopg/docs/advanced.html#asynchronous-notifications
# - http://wiki.postgresql.org/wiki/PgBouncer
# - https://github.com/wulczer/txpostgres
# - http://www.postgresql.org/docs/9.1/static/tutorial-transactions.html
# - https://bitbucket.org/descent/psytornet/overview
# - An ORM? Look here: https://github.com/coleifer/peewee
# - http://initd.org/psycopg/docs/advanced.html#asynchronous-support

# Schedule
#
# 1. Working state (similar functionality as stable Momoko)
#    * execute
#    * Exception handling!
#    * callproc
#    * mogrify
#    - batch (can be done with gen)
#    - chain (can be done with gen)
#    * clean pool
#    * close pool
#    * create new connection if none is free
#    * recover from broken connection
# 2. Implement Psycopg2 functionality
#    - Everything that works in asynchronous mode.
#    - Replacements for things that do not work in asynchronous mode
# 3. Transactions (included in point 2, but this is a more advanced feature)
# 4. Aynchronous notifications (included in point 2, but this is a more advanced feature)
#    Can be done manually with BEGIN and COMMIT. Maybe introduce a convenience function?
#    Does anyone use this?
# 5. Investigate and test PgBouncer
# 6. Investigate txpostgres and psytornet
# 7. Investigate and try making an asynchronous ORM suitable for callback based database drivers


from functools import partial
from collections import deque

from tornado import gen
from tornado.ioloop import IOLoop, PeriodicCallback

from .utils import Op, psycopg2, log, transaction
from .exceptions import PoolError


TRANSACTION_STATUS_IDLE = psycopg2.extensions.TRANSACTION_STATUS_IDLE


# The dummy callback is used to keep the asynchronous cursor alive in case no
# callback has been specified. This will prevent the cursor from being garbage
# collected once, for example, ``ConnectionPool.execute`` has finished.
def _dummy_callback(cursor, error):
    pass


class BConnectionPool(object):
    """
    Blocking connection pool acting as a single connection.
    """
    pass


class ConnectionPool(object):
    """
    Asynchronous connection pool acting as a single connection.

    `dsn` and `connection_factory` are passed to `momoko.connection.Connection`
    when a new connection is created. It also contains the documentation about
    these two parameters.

    - **minconn** --
        Amount of connection created upon initialization.
    - **maxconn** --
        Maximum amount of connections supported by the pool.
    - **cleanup_timeout** --
        Time in seconds between pool cleanups. Unused connections
        are closed and removed from the pool until only `minconn` are left. When
        an integer below `1` is used the pool cleaner will be disabled.
    - **ioloop** --
        An instance of Tornado's IOLoop.
    """
    def __init__(self, dsn, connection_factory=None, minconn=1, maxconn=5,
                 cleanup_timeout=10, ioloop=None):
        self._dsn = dsn
        self._minconn = minconn
        self._maxconn = maxconn
        self._connection_factory = connection_factory
        self._ioloop = ioloop or IOLoop.instance()
        self.closed = False
        self._pool = []

        # Create connections
        for i in range(self._minconn):
            self._new_connection()

        # Create a periodic callback that tries to close inactive connections
        if cleanup_timeout > 0:
            self._cleaner = PeriodicCallback(self._clean_pool,
                cleanup_timeout * 1000)
            self._cleaner.start()

    def _new_connection(self, callback=None):
        if len(self._pool) > self._maxconn:
            raise PoolError('connection pool exausted')

        connection = Connection(ioloop=self._ioloop)
        if callback is not None:
            callbacks = [
                partial(callback, connection),
                lambda error: self._pool.append(connection)]
        else:
            callbacks = [lambda error: self._pool.append(connection)]

        connection.open(self._dsn, self._connection_factory, callbacks)

    def _get_connection(self):
        """
        Look for a free connection.
        """
        if self.closed:
            raise PoolError('connection pool is closed')

        free_connection = None
        for connection in self._pool:
            if not connection.busy:
                free_connection = connection
                break

        return free_connection

    def _clean_pool(self):
        """
        Close a number of inactive connections when the number of connections
        in the pool exceeds the number in `min_conn`.
        """
        if self.closed:
            raise PoolError('connection pool is closed')

        pool_len = len(self._pool)
        if pool_len > self._minconn:
            overflow = pool_len - self._minconn
            to_be_removed = [i for i in self._pool if not i.busy]
            for i in to_be_removed[:overflow]:
                self._pool.remove(i)
                i.close()

    def transaction(self, statements, cursor_factory=None, callback=None, connection=None, error=None):
        connection = connection or self._get_connection()
        if connection is None:
            self._new_connection(partial(self.transaction, statements,
                cursor_factory, callback))
            return

        transaction(connection, statements, cursor_factory, callback)

    def execute(self, operation, parameters=(), cursor_factory=None,
                callback=_dummy_callback, retries=5, connection=None, error=None):
        connection = connection or self._get_connection()
        if connection is None:
            self._new_connection(partial(self.execute, operation,
                parameters, cursor_factory, callback, retries))
            return

        try:
            connection.execute(operation, parameters, cursor_factory, callback)
        except (psycopg2.Warning, psycopg2.Error) as e:
            log.error('An error occurred: {0}'.format(e))
            if retries == 0:
                raise e
            self._pool.remove(connection)
            self._ioloop.add_callback(partial(self.execute, operation,
                parameters, cursor_factory, callback, retries-1))

    def callproc(self, procname, parameters=(), cursor_factory=None,
                callback=_dummy_callback, retries=5, connection=None, error=None):
        connection = connection or self._get_connection()
        if connection is None:
            self._new_connection(partial(self.callproc, operation,
                parameters, cursor_factory, callback, retries))
            return

        try:
            connection.callproc(procname, parameters, cursor_factory, callback)
        except (psycopg2.Warning, psycopg2.Error) as e:
            log.error('An error occurred: {0}'.format(e))
            if retries == 0:
                raise e
            self._pool.remove(connection)
            self._ioloop.add_callback(partial(self.callproc, operation,
                parameters, cursor_factory, callback, retries-1))

    def mogrify(self, operation, parameters=(), callback=_dummy_callback,
                retries=5, connection=None, error=None):
        connection = connection or self._get_connection()
        if connection is None:
            self._new_connection(partial(self.mogrify, operation, parameters,
                callback, retries))
            return

        try:
            connection.mogrify(operation, parameters, callback)
        except (psycopg2.Warning, psycopg2.Error) as e:
            log.error('An error occurred: {0}'.format(e))
            if retries == 0:
                raise e
            self._pool.remove(connection)
            self._ioloop.add_callback(partial(self.mogrify, operation,
                parameters, callback, retries-1))

    def close(self):
        if self.closed:
            raise PoolError('connection pool is already closed')

        for conn in self._pool:
            if not conn.closed:
                conn.close()

        self._cleaner.stop()
        self._pool = []
        self.closed = True


class Connection(object):
    """
    Asynchronous connection class that wraps a Psycopg2 connection.

    If both `channel` and `notify_callback` are set the connection is going to
    listen to the specified channel and `notify_callback` will be executed.
    An instance of [Notify][1] will be passed to the callback.

    - **channel** -- The name of the channel that the connection listens to.
    - **notify_callback** --
        A callable that is called when a notification is received.
    - **ioloop** -- An instance of Tornado's IOLoop.

    [1]: http://initd.org/psycopg/docs/extensions.html#psycopg2.extensions.Notify
    """
    def __init__(self, channel=None, notify_callback=None, ioloop=None):
        self._connection = None
        self._fileno = None
        self._ioloop = ioloop or IOLoop.instance()
        self._callbacks = []

        # Shortcut for getting the transaction status
        self._transaction_status = lambda: False

        if channel and notify_callback:
            if not channel.isidentifier():
                raise ValueError(
                    'A channel name can only contain the uppercase and '
                    'lowercase letters A through Z, the underscore _ and, '
                    'except for the first character, the digits 0 through 9.')

            if notify_callback and not callable(notify_callback):
                raise TypeError('notify_callback must be callable!')

        self._channel = channel
        self._notify_callback = notify_callback

    def _io_callback(self, fd, events):
        try:
            error = None
            state = self._connection.poll()
        except (psycopg2.Warning, psycopg2.Error) as e:
            error = e
            state = psycopg2.extensions.POLL_OK

        if state == psycopg2.extensions.POLL_OK:
            for callback in self._callbacks:
                callback(error)
        elif state == psycopg2.extensions.POLL_READ:
            self._ioloop.update_handler(self._fileno, IOLoop.READ)
        elif state == psycopg2.extensions.POLL_WRITE:
            self._ioloop.update_handler(self._fileno, IOLoop.WRITE)

    def open(self, dsn, connection_factory=None, callbacks=[]):
        """
        Open an asynchronous connection.

        - **dsn** --
            A [Data Source Name][1] string containing one of the collowing values:

            + **dbname** - the database name
            + **user** - user name used to authenticate
            + **password** - password used to authenticate
            + **host** - database host address (defaults to UNIX socket if not provided)
            + **port** - connection port number (defaults to 5432 if not provided)

            Or any other parameter supported by PostgreSQL. See the PostgreSQL
            documentation for a complete list of supported [parameters][2].

        - **connection_factory** --
            The `connection_factory` argument can be used to create non-standard
            connections. The class returned should be a subclass of
            [psycopg2.extensions.connection][3].

        - **callbacks** --
            Sequence of callables. These are executed after the connection has
            been established.

        [1]: http://en.wikipedia.org/wiki/Data_Source_Name
        [2]: http://www.postgresql.org/docs/current/static/libpq-connect.html#LIBPQ-PQCONNECTDBPARAMS
        [3]: http://initd.org/psycopg/docs/connection.html#connection
        """
        args = []
        if not connection_factory is None:
          args.append(connection_factory)
        self._connection = psycopg2.connect(dsn, *args, async=1)

        self._transaction_status = self._connection.get_transaction_status
        self._fileno = self._connection.fileno()
        self._callbacks = callbacks

        if self._channel and self._notify_callback:
            self._callbacks.append(self._setup_notify)

        # Set connection state
        self._ioloop.add_handler(self._fileno, self._io_callback, IOLoop.WRITE)

    def _setup_notify(self, error):
        self.execute('LISTEN {0};'.format(self._channel), callback=partial(self._poll_notify))

    def _poll_notify(self, cursor, error):
        while self._connection.notifies:
            notify = self._connection.notifies.pop()
            self._notify_callback(notify)

        # Set callback and connection state
        self._callbacks = [partial(self._poll_notify, cursor)]
        self._ioloop.update_handler(self._fileno, IOLoop.WRITE)

    def close(self):
        """
        Close asynchronous connection.
        """
        self._ioloop.remove_handler(self._fileno)
        self._connection.close()

    def execute(self, operation, parameters=(), cursor_factory=None,
                callback=_dummy_callback):
        cursor = self._connection.cursor(cursor_factory=
            cursor_factory or psycopg2.extensions.cursor)
        cursor.execute(operation, parameters)

        # Set callback and connection state
        self._callbacks = [partial(callback, cursor)]
        self._ioloop.update_handler(self._fileno, IOLoop.WRITE)

    def callproc(self, procname, parameters=(), cursor_factory=None,
                callback=_dummy_callback):
        cursor = self._connection.cursor(cursor_factory=
            cursor_factory or psycopg2.extensions.cursor)
        cursor.callproc(procname, parameters)

        # Set callback and connection state
        self._callbacks = [partial(callback, cursor)]
        self._ioloop.update_handler(self._fileno, IOLoop.WRITE)

    def mogrify(self, operation, parameters=(), callback=_dummy_callback):
        cursor = self._connection.cursor()
        result = cursor.mogrify(operation, parameters)
        self._ioloop.add_callback(partial(callback, result, None))

    @property
    def busy(self):
        return self._connection.isexecuting() or \
            self._transaction_status() != TRANSACTION_STATUS_IDLE

    @property
    def closed(self):
        """
        Read-only attribute reporting whether the database connection is
        open (`False`) or closed (`True`).
        """
        return self._connection.closed == 1
