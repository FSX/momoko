TODO
====

A list of items that have to be added and ideas.


Asynchronous Notifications
--------------------------

About: Psycopg2 `Asynchronous Notifications`_

Psycopg allows asynchronous interaction with other database sessions using the
facilities offered by PostgreSQL commands ``LISTEN`` and ``NOTIFY``.

With the current API this is not possible. It is, but not with the public API.
And how would it be used? In a request handler, a separate process/thread or
fire a callback when something is received? It would need it's own connection,
otherwise it would make things unnecessarily complex.

Some use cases or people actually using this PostgreSQL feature in a production
setup would give a good perspective on how this is used and why.


Transactions
------------

About: Transactions_

The API for transactions in Psycopg2 (``commit`` and ``rollback``) is not
available in asynchronous mode. Asynchronous connections are in autocommit mode,
meaning that SQL queries are executed and committed directly with no possible
rollback.

Transactions are possible with the ``BEGIN``, ``COMMIT`` and ``ROLLBACK`` SQL
commands and Tornado gen module makes it easier to run SQL queries sequentially.
The sequence of queries also need to use the same connection.

A wrapper needs to be made for this.


COPY TO and COPY FROM
---------------------

About: `COPY TO and COPY FROM`_

The section about `Asynchronous Support`_ states the following:

	COPY commands are not supported either in asynchronous mode, but this
	will be probably implemented in a future release.

The following function in the C code can be examined and maybe adapted to support
asynchronous mode if this functionality is really needed.

- `psyco_curs_copy_from`_
- `psyco_curs_copy_to`_
- `_psyco_curs_copy_columns`_


Named cursors
-------------

Named cursor (or server-side cursors) are not supported in asynchronous mode,
because it involves extra SQL commands internally_. One can manually use
names/server-side cursor by using DECLARE_, FETCH_ and MOVE_.

This can be implemented by using in Momoko itself in Python, but not Psycopg2.


http://www.postgresql.org/docs/8.1/static/plpgsql-cursors.html

.. _Asynchronous Notifications: http://initd.org/psycopg/docs/advanced.html#asynchronous-notifications
.. _Transactions: http://initd.org/psycopg/docs/usage.html#transactions-control
.. _COPY TO and COPY FROM: http://initd.org/psycopg/docs/usage.html#using-copy-to-and-copy-from
.. _Asynchronous Support: http://initd.org/psycopg/docs/advanced.html#asynchronous-support

.. _psyco_curs_copy_from: https://github.com/dvarrazzo/psycopg/blob/devel/psycopg/cursor_type.c#L1291
.. _psyco_curs_copy_to: https://github.com/dvarrazzo/psycopg/blob/devel/psycopg/cursor_type.c#L1391
.. __psyco_curs_copy_columns: https://github.com/dvarrazzo/psycopg/blob/devel/psycopg/cursor_type.c#L1189

.. _internally: https://github.com/dvarrazzo/psycopg/blob/devel/psycopg/cursor_type.c
.. _DECLARE: http://www.postgresql.org/docs/8.1/static/sql-declare.html
.. _FETCH: http://www.postgresql.org/docs/9.1/static/sql-fetch.html
.. _MOVE: http://www.postgresql.org/docs/9.1/static/sql-move.html
