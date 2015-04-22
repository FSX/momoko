# -*- coding: utf-8 -*-
"""
momoko.exceptions
=================

Exceptions.

Copyright 2011-2014, Frank Smit & Zaar Hai.
MIT, see LICENSE for more details.
"""


class PoolError(Exception):
    """
    Raised when something goes wrong in the connection pool.
    """
    pass


class PartiallyConnected(PoolError):
    """
    Raised  when :py:meth:`momoko.Pool` can initialize all requested connections.
    """