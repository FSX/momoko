Momoko
======

.. image:: https://img.shields.io/pypi/v/momoko.svg
    :target: https://pypi.python.org/pypi/momoko

.. image:: https://img.shields.io/pypi/dm/momoko.svg
        :target: https://pypi.python.org/pypi/momoko

Momoko wraps Psycopg2_'s functionality for use in Tornado_. Have a look at tutorial_ or full documentation_.

.. _Psycopg2: http://initd.org/psycopg/
.. _Tornado: http://www.tornadoweb.org/
.. _tutorial: http://momoko.61924.nl/en/latest/tutorial.html
.. _documentation: http://momoko.61924.nl/en/latest/


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
