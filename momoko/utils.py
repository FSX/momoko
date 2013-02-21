# -*- coding: utf-8 -*-
"""
momoko.utils
============

Utilities that make life easier.

Copyright 2011-2012 by Frank Smit.
MIT, see LICENSE for more details.
"""

import sys
import logging
from tornado import gen
from functools import partial
from collections import deque


if sys.version_info[0] < 3:
    is_python_3k = False
else:
    is_python_3k = True


log = logging.getLogger('momoko')


class Op(gen.Task):
    """
    Run a single asynchronous operation.

    Behaves like `tornado.gen.Task`_, but raises an exception (one of Psycop2's
    exceptions_) when an error occurs related to Psycopg2 or PostgreSQL.

    .. _exceptions: http://initd.org/psycopg/docs/module.html#exceptions
    .. _tornado.gen.Task: http://www.tornadoweb.org/documentation/gen.html#tornado.gen.Task
    """
    def get_result(self):
        (result, error), _ = super(Op, self).get_result()
        if error:
            raise error
        return result


class WaitOp(gen.Wait):
    """
    Return the argument passed to the result of a previous `tornado.gen.Callback`_.

    Behaves like `tornado.gen.Wait`_, but raises an exception (one of Psycop2's
    exceptions_) when an error occurs related to Psycopg2 or PostgreSQL.

    .. _exceptions: http://initd.org/psycopg/docs/module.html#exceptions
    .. _tornado.gen.Callback: http://www.tornadoweb.org/documentation/gen.html#tornado.gen.Callback
    .. _tornado.gen.Wait: http://www.tornadoweb.org/documentation/gen.html#tornado.gen.Wait
    """
    def get_result(self):
        (result, error), _ = super(WaitOp, self).get_result()
        if error:
            raise error
        return result


class WaitAllOps(gen.WaitAll):
    """
    Return the results of multiple previous `tornado.gen.Callback`_.

    Behaves like `tornado.gen.WaitAll`_, but raises an exception (one of Psycop2's
    exceptions_) when an error occurs related to Psycopg2 or PostgreSQL.

    .. _exceptions: http://initd.org/psycopg/docs/module.html#exceptions
    .. _tornado.gen.Callback: http://www.tornadoweb.org/documentation/gen.html#tornado.gen.Callback
    .. _tornado.gen.WaitAll: http://www.tornadoweb.org/documentation/gen.html#tornado.gen.WaitAll
    """
    def get_result(self):
        super_results = super(WaitAllOps, self).get_result()

        results = []
        for (result, error), _ in super_results:
            if error:
                raise error
            else:
                results.append(result)

        return results
