# -*- coding: utf-8 -*-
"""
    momoko
    ~~~~~~

    Asynchronous Psycopg wrapper for Tornado.

    :copyright: (c) 2011-2012 by Frank Smit.
    :license: MIT, see LICENSE for more details.
"""

import psycopg2
from psycopg2 import ProgrammingError

from .connection import ConnectionPool, BConnectionPool, Connection
from .exceptions import PoolError
from .utils import Op, WaitOp, WaitAllOps


try:
    psycopg2.extensions.POLL_OK
except AttributeError:
    import warnings
    warnings.warn(RuntimeWarning(
            'psycopg2 does not have async support. '
            'You need at least version 2.2.0 of psycopg2 '
            'to use Momoko.'))
