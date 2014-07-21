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
    The ``PoolError`` exception is raised when something goes wrong in the connection
    pool. When the maximum amount is exceeded for example.
    """
    pass
