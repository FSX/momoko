# -*- coding: utf-8 -*-
"""
momoko.utils
============

Extra functionality.

Copyright 2011-2012 by Frank Smit.
MIT, see LICENSE for more details.
"""


import sys
from tornado import gen
from functools import partial
from collections import deque


if sys.version_info[0] < 3:
    is_python_3k = False
else:
    is_python_3k = True


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
