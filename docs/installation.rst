.. _installation:

Installation
============

Momoko supports Python 2 and 3 and PyPy with psycopg2ct_ or psycopg2cffi_.
And the only dependencies are Tornado_ and Psycopg2_. Installation is easy
using *easy_install* or pip_::

    pip install momoko

The lastest source code can always be cloned from the `Github repository`_ with::

    git clone git://github.com/FSX/momoko.git
    cd momoko
    python setup.py install

The unit tests can be run before anything is installed to ensure everything is
working. Before running the unit tests a PostgreSQL database needs to be set up
and some environment variables need to be set. The dependencies will be installed
automatically.

Here's an example for the environment variables::

    export MOMOKO_TEST_DB='your_db'
    export MOMOKO_TEST_USER='your_user'
    export MOMOKO_TEST_PASSWORD='your_password'
    export MOMOKO_TEST_HOST='localhost'
    export MOMOKO_TEST_PORT='5432'

    # Set to '0' if hstore extension isn't enabled
    export MOMOKO_TEST_HSTORE='1'

And running the tests is as easy as running the following command::

   python setup.py test


.. _psycopg2ct: http://pypi.python.org/pypi/psycopg2ct
.. _psycopg2cffi: http://pypi.python.org/pypi/psycopg2cffi
.. _Tornado: http://www.tornadoweb.org/
.. _Psycopg2: http://initd.org/psycopg/
.. _pip: http://www.pip-installer.org/
.. _Github repository: https://github.com/FSX/momoko
