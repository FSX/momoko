.. _installation:

Installation
============

Momoko supports Python 2 and 3 and PyPy with psycopg2cffi_.
And the only dependencies are Tornado_ and Psycopg2_ (or psycopg2cffi_).
Installation is easy using *easy_install* or pip_::

    pip install momoko

The lastest source code can always be cloned from the `Github repository`_ with::

    git clone git://github.com/FSX/momoko.git
    cd momoko
    python setup.py install

Psycopg2 is used by default when installing Momoko, but psycopg2cffi
can also be used by setting the ``MOMOKO_PSYCOPG2_IMPL`` environment variable to
``psycopg2cffi`` before running ``setup.py``. For example::

    # 'psycopg2' or 'psycopg2cffi'
    export MOMOKO_PSYCOPG2_IMPL='psycopg2cffi'

The unit tests all use this variable. It needs to be set if something else is used
instead of Psycopg2 when running the unit tests. Besides ``MOMOKO_PSYCOPG2_IMPL``
there are also other variables that need to be set for the unit tests.

Here's an example for the environment variables::

    export MOMOKO_TEST_DB='your_db'  # Default: momoko_test
    export MOMOKO_TEST_USER='your_user'  # Default: postgres
    export MOMOKO_TEST_PASSWORD='your_password'  # Empty de default
    export MOMOKO_TEST_HOST='localhost'  # Empty de default
    export MOMOKO_TEST_PORT='5432'  # Default: 5432

    # Set to '0' if hstore extension isn't enabled
    export MOMOKO_TEST_HSTORE='1'  # Default: 0

Momoko tests use tcproxy_ for simulating Postgres server unavailablity. The copy
of tcproxy is bundled with Momoko, but you need to build it first::

    make -C tcproxy

Finally, running the tests is easy::

   python setup.py test


.. _tcproxy: https://github.com/dccmx/tcproxy
.. _psycopg2cffi: http://pypi.python.org/pypi/psycopg2cffi
.. _Tornado: http://www.tornadoweb.org/
.. _Psycopg2: http://initd.org/psycopg/
.. _pip: http://www.pip-installer.org/
.. _Github repository: https://github.com/FSX/momoko
