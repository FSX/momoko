# -*- coding: utf-8 -*-
"""
    momoko.utils
    ~~~~~~~~~~~~

    Utilities for Momoko.

    :copyright: (c) 2011 by Frank Smit.
    :license: MIT, see LICENSE for more details.
"""


import functools

import psycopg2
import psycopg2.extensions
from tornado.ioloop import IOLoop


class TransactionChain(object):
    """Run queries as a transaction

    A list/tuple with queries looks like this::

        (
            ['SELECT 42, 12, %s, 11;', (23,)],
            'SELECT 1, 2, 3, 4, 5;'
        )

    A query with parameters is contained in a list: ``['some sql
    here %s, %s', ('and some', 'parameters here')]``. A query
    without parameters doesn't need to be in a list.

    :param db: A ``momoko.Client`` or ``momoko.AdispClient`` instance.
    :param statements: A tuple or with all the queries.
    :param callback: The function that needs to be executed once all the
                     queries are finished.
    :param cursor_kwargs: A dictionary with Psycopg's `connection.cursor`_ arguments.
    :return: A list with the resulting cursors is passed on to the callback.

    .. _connection.cursor: http://initd.org/psycopg/docs/connection.html#connection.cursor
    """
    def __init__(self, db, statements, callback, cursor_kwargs={}):
        self._db = db
        self._cursors = []
        self._statements = list(statements)
        self._statements.reverse()
        self._callback = callback
        self._cursor_kwargs = cursor_kwargs
        self._db._pool.get_connection(self._set_connection)

    def _set_connection(self, conn):
        self._connection = conn
        self._db._pool._pool.remove(conn) # don't let other connections mess up the transaction
        self._collect(None)

    def _collect(self, cursor):
        if cursor is not None:
            self._cursors.append(cursor)
        if not self._statements:
            if self._callback:
                self._callback(self._cursors)
            self._db._pool._pool.append(self._connection)
            return
        statement = self._statements.pop()
        if isinstance(statement, str):
            statement = [statement]
        self._db.execute(*statement, callback=self._collect,
            cursor_kwargs=self._cursor_kwargs, connection=self._connection)


class QueryChain(object):
    """Run a chain of queries in the given order.

    A list/tuple with queries looks like this::

        (
            ['SELECT 42, 12, %s, 11;', (23,)],
            'SELECT 1, 2, 3, 4, 5;'
        )

    A query with parameters is contained in a list: ``['some sql
    here %s, %s', ('and some', 'parameters here')]``. A query
    without parameters doesn't need to be in a list.

    :param db: A ``momoko.Client`` or ``momoko.AdispClient`` instance.
    :param queries: A tuple or with all the queries.
    :param callback: The function that needs to be executed once all the
                     queries are finished.
    :param cursor_kwargs: A dictionary with Psycopg's `connection.cursor`_ arguments.
    :return: A list with the resulting cursors is passed on to the callback.

    .. _connection.cursor: http://initd.org/psycopg/docs/connection.html#connection.cursor
    """
    def __init__(self, db, queries, callback, cursor_kwargs={}):
        self._db = db
        self._cursors = []
        self._queries = list(queries)
        self._queries.reverse()
        self._callback = callback
        self._cursor_kwargs = cursor_kwargs
        self._collect(None)

    def _collect(self, cursor):
        if cursor is not None:
            self._cursors.append(cursor)
        if not self._queries:
            if self._callback:
                self._callback(self._cursors)
            return
        query = self._queries.pop()
        if isinstance(query, str):
            query = [query]
        self._db.execute(*query, callback=self._collect,
            cursor_kwargs=self._cursor_kwargs)


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

    A query with parameters is contained in a list: ``['some sql
    here %s, %s', ('and some', 'parameters here')]``. A query
    without parameters doesn't need to be in a list.

    :param db: A ``momoko.Client`` or ``momoko.AdispClient`` instance.
    :param queries: A dictionary with all the queries.
    :param callback: The function that needs to be executed once all the
                     queries are finished.
    :param cursor_kwargs: A dictionary with Psycopg's `connection.cursor`_ arguments.
    :return: A dictionary with the same keys as the given queries with the
             resulting cursors as values is passed on to the callback.

    .. _connection.cursor: http://initd.org/psycopg/docs/connection.html#connection.cursor
    """
    def __init__(self, db, queries, callback, cursor_kwargs={}):
        self._db = db
        self._callback = callback
        self._cursor_kwargs = cursor_kwargs
        self._queries = {}
        self._args = {}
        self._size = len(queries)

        for key, query in list(queries.items()):
            if isinstance(query, str):
                query = [query, ()]
            query.append(functools.partial(self._collect, key))
            self._queries[key] = query

        for query in list(self._queries.values()):
            self._db.execute(*query, cursor_kwargs=self._cursor_kwargs)

    def _collect(self, key, cursor):
        self._size -= 1
        self._args[key] = cursor
        if not self._size and self._callback:
            self._callback(self._args)
