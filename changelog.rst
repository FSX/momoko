Changelog
=========

2.2.1 (2015-10-13)
------------------
*  Wait for pending connections during connection acquiring (`issue 122`_). Thanks to jbowes_.

.. _issue 122: https://github.com/FSX/momoko/issues/122
.. _jbowes: https://github.com/jbowes

2.2.0 (2015-09-20)
------------------
*  Fixed serious flaw with connection retrials. `More details`_.
*  Fixed ping to handle failures properly (`issue 115`_).
*  NOTE: gcc is now required to run tests - we supply built-in version of `tcproxy`_ for connection failure simulation.

.. _More details: https://github.com/FSX/momoko/commit/85183f5370181f75a29e876f5211d99c40b4ba5e
.. _issue 115: https://github.com/FSX/momoko/issues/115
.. _tcproxy: https://github.com/dccmx/tcproxy

2.1.1 (2015-08-03)
------------------
*  Fixed JSON/HSTORE support with named cursors (`issue 112`_). Thanks to helminster_.

.. _issue 112: https://github.com/FSX/momoko/issues/112
.. _helminster: https://github.com/helminster

2.1.0 (2015-07-08)
------------------
*  Auto shrink support. Thanks to `John Chumnanvech`_.

.. _John Chumnanvech: https://github.com/jchumnanvech

2.0.0 (2015-05-10)
------------------
*  Full rewrite using using Futures_
*  NOTE: The new API is similar but not backwards compatible. Make sure to read documentation first.

.. _Futures: http://tornado.readthedocs.org/en/latest/concurrent.html

1.1.6 (2015-04-26)
------------------
*  Aadded register_json
*  Docs: fix typos, spelling, grammatical errors; improve unclear wording
*  Removed support for psycopg2ct


1.1.5 (2014-11-17)
------------------

*  Catching ALL types of early error. Fixes `issue 79`_.

.. _issue 79: https://github.com/FSX/momoko/issues/79


1.1.4 (2014-07-21)
------------------

*  Tornado 4.0 compatablity: backported old ``Task`` class for Tornado 4.0 compatablity.


1.1.3 (2014-05-21)
------------------

* Fixed hstore.


1.1.2 (2014-03-06)
------------------

* Fixed a minor Python 3.2 issue.


1.1.1 (2014-03-06)
------------------

Fixes:

* ``Connection.transaction`` does not break when passed SQL strings are of ``unicode`` type


1.1.0 (2014-02-24)
------------------

New features:

* Transparent automatic reconnects if database disappears and comes back.
* Session init commands (``setsession``).
* Dynamic pool size stretching. New connections will be opened under
  load up-to predefined limit.
* API for manual connection management with ``getconn``/``putconn``. Useful for server-side cursors.
* A lot of internal improvements and cleanup.

Fixes:

* Connections are managed explicitly - eliminates transaction problems reported.
* ``connection_factory`` (and ``curosr_factor``) arguments handled properly by ``Pool``.


1.0.0 (2013-05-01)
------------------

* Fix code example in documentation. By matheuspatury in `pull request 46`_

.. _pull request 46: https://github.com/FSX/momoko/pull/46


1.0.0b2 (2013-02-28)
--------------------

* Tested on CPython 2.6, 2.7, 3.2, 3.3 and PyPy with Psycopg2_, psycopg2ct_ and psycopg2cffi_.
* Add and remove a database connection to and from the IOLoop for each operation.
  See `pull request 38`_ and commits 189323211b_ and 92940db0a0_ for more information.
* Replaced dynamic connection pool with a static one.
* Add support for hstore_.

.. _Psycopg2: http://initd.org/psycopg/
.. _psycopg2ct: http://pypi.python.org/pypi/psycopg2ct
.. _psycopg2cffi: http://pypi.python.org/pypi/psycopg2cffi
.. _pull request 38: https://github.com/FSX/momoko/pull/38
.. _189323211b: https://github.com/FSX/momoko/commit/189323211bcb44ea158f41ddf87d4240c0e657d6
.. _92940db0a0: https://github.com/FSX/momoko/commit/92940db0a0f6d780724f42d3d66f1b75a78430ff
.. _hstore: http://www.postgresql.org/docs/9.2/static/hstore.html


1.0.0b1 (2012-12-16)
--------------------

This is a beta release. It means that the code has not been tested thoroughly
yet. This first beta release is meant to provide all the functionality of the
previous version plus a few additions.

* Most of the code has been rewritten.
* The mogrify_ method has been added.
* Added support for transactions.
* The query chain and batch have been removed, because ``tornado.gen`` can be used instead.
* Error reporting has bee improved by passing the raised exception to the callback.
  A callback accepts two arguments: the cursor and the error.
* ``Op``, ``WaitOp`` and ``WaitAllOps`` in ``momoko.utils`` are wrappers for
  classes in ``tornado.gen`` which raise the error again when one occurs.
  And the user can capture the exception in the request handler.
* A complete set of tests has been added in the ``momoko`` module: ``momoko.tests``.
  These can be run with ``python setup.py test``.

.. _mogrify: http://initd.org/psycopg/docs/cursor.html#cursor.mogrify


0.5.0 (2012-07-30)
------------------

* Removed all Adisp related code.
* Refactored connection pool and connection polling.
* Just pass all unspecified arguments to ``BlockingPool`` and ``AsyncPool``. So
  ``connection_factory`` can be used again.


0.4.0 (2011-12-15)
------------------

* Reorganized classes and files.
* Renamed ``momoko.Client`` to ``momoko.AsyncClient``.
* Renamed ``momoko.Pool`` to ``momoko.AsyncPool``.
* Added a client and pool for blocking connections, ``momoko.BlockingClient``
  and ``momoko.BlockingPool``.
* Added ``PoolError`` to the import list in ``__init__.py``.
* Added an example that uses Tornado's gen_ module and Swift_.
* Callbacks are now optional for ``AsyncClient``.
* ``AsyncPool`` and ``Poller`` now accept a ioloop argument. [fzzbt_]
* Unit tests have been added. [fzzbt_]

.. _gen: http://www.tornadoweb.org/documentation/gen.html
.. _Swift: http://code.naeseth.com/swirl/
.. _fzzbt: https://github.com/fzzbt


0.3.0 (2011-08-07)
------------------

* Renamed ``momoko.Momoko`` to ``momoko.Client``.
* Programming in blocking-style is now possible with ``AdispClient``.
* Support for Python 3 has been added.
* The batch and chain fucntion now accepts different arguments. See the
  documentation for details.


0.2.0 (2011-04-30)
------------------

* Removed ``executemany`` from ``Momoko``, because it can not be used in asynchronous mode.
* Added a wrapper class, ``Momoko``, for ``Pool``, ``BatchQuery`` and ``QueryChain``.
* Added the ``QueryChain`` class for executing a chain of queries (and callables)
  in a certain order.
* Added the ``BatchQuery`` class for executing batches of queries at the same time.
* Improved ``Pool._clean_pool``. It threw an ``IndexError`` when more than one
  connection needed to be closed.


0.1.0 (2011-03-13)
-------------------

* Initial release.
