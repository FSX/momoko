# -*- coding: utf-8 -*-
"""
momoko.utils
============

Extra functionality.

Copyright 2011-2012 by Frank Smit.
MIT, see LICENSE for more details.
"""


import sys
import logging
from tornado import gen
from functools import partial


if sys.version_info[0] < 3:
    is_python_3k = False
else:
    is_python_3k = True


log = logging.getLogger('momoko')


try:
    import psycopg2
except ImportError:
    if not is_python_3k:
        try:
            import psycopg2ct as psycopg2
        except ImportError:
            raise ImportError('no module named psycopg2 or psycopg2ct')
    else:
        raise ImportError('no module named psycopg2')


class Op(gen.Task):
    def get_result(self):
        (result, error), _ = super(Op, self).get_result()
        if error:
            raise error
        return result


class WaitOp(gen.Wait):
    def get_result(self):
        (result, error), _ = super(WaitOp, self).get_result()
        if error:
            raise error
        return result


class WaitAllOps(gen.WaitAll):
    def get_result(self):
        super_results = super(WaitAllOps, self).get_result()

        results = []
        for (result, error), _ in super_results:
            if error:
                raise error
            else:
                results.append(result)

        return results


def transaction(connection, statements, cursor_factory=None, callback=None):
    statements = ['COMMIT;'] + list(reversed(statements)) + ['BEGIN;']
    cursors = []

    def error_callback(transaction_error, error):
        callback(None, error or transaction_error)

    def process(cursor=None, error=None):
        if error:
            return connection.execute('ROLLBACK;',
                cursor_factory=cursor_factory,
                callback=partial(error_cb, error_callback))
        if cursor:
            cursors.append(cursor)

        if not statements:
            return callback(cursors[1:-1], None)

        statement = statements.pop()
        if isinstance(statement, str):
            statement = [statement]

        connection.execute(*statement,
            cursor_factory=cursor_factory,
            callback=process)

    process()
