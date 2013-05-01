Changelog
=========

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

.. _Psycopg2: http://www.initd.org/psycopg/
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
