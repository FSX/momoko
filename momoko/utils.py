# -*- coding: utf-8 -*-
"""
momoko.utils
============

Utilities that make life easier.

Copyright 2011-2014, Frank Smit & Zaar Hai.
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


class Task(gen.YieldPoint):
    """Runs a single asynchronous operation.

    Takes a function (and optional additional arguments) and runs it with
    those arguments plus a ``callback`` keyword argument.  The argument passed
    to the callback is returned as the result of the yield expression.

    A `Task` is equivalent to a `Callback`/`Wait` pair (with a unique
    key generated automatically)::

        result = yield gen.Task(func, args)

        func(args, callback=(yield gen.Callback(key)))
        result = yield gen.Wait(key)
    """
    def __init__(self, func, *args, **kwargs):
        assert "callback" not in kwargs
        self.args = args
        self.kwargs = kwargs
        self.func = func

    def start(self, runner):
        self.runner = runner
        self.key = object()
        runner.register_callback(self.key)
        self.kwargs["callback"] = runner.result_callback(self.key)
        self.func(*self.args, **self.kwargs)

    def is_ready(self):
        return self.runner.is_ready(self.key)

    def get_result(self):
        return self.runner.pop_result(self.key)


class Op(Task):
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
