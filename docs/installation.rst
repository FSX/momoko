.. _installation:

Installation
============

Momoko supports Python 2 and 3 (tested with 2.7 and 3.2). PyPy is not
supported, because there is no usable Psycopg2 module for PyPy.

Momoko only depends on two modules. Tornado_ and and Psycopg2_ (2.2.0 or higher).
Psycopg2 must have support for asynchronous connections.

Momoko can be installed with *easy_install* or pip_::

    pip install momoko

The latest sources can be cloned from the `Github repository`_ and
installed with::

    python setup.py install


.. _Tornado: http://www.tornadoweb.org/
.. _Psycopg2: http://initd.org/psycopg/
.. _pip: http://www.pip-installer.org/
.. _Github repository: https://github.com/FSX/momoko
