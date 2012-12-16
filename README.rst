Momoko
======

A Tornado_ wrapper for Psycopg2_.

.. _Psycopg2: http://www.initd.org/psycopg/
.. _Tornado: http://www.tornadoweb.org/


Installation
------------

With pip::

    pip install momoko

Or manually::

    python setup.py install


Testing
-------

Set the following environment variables with your own values before running the
unit tests::

    export MOMOKO_TEST_DB='your_db'
    export MOMOKO_TEST_USER='your_user'
    export MOMOKO_TEST_PASSWORD='your_password'
    export MOMOKO_TEST_HOST='localhost'
    export MOMOKO_TEST_PORT='5432'

And run the tests with::

    python setup.py test
