Installation
============

Momoko supports Python 2 and 3 (tested with 2.7 and 3.2). PyPy is not
supported, because there is no usable Psycopg2 module for PyPy.

Besides Python itself only two other modules are needed. Tornado_ and
Psycopg2_. The Psycopg2 module must have support for asynchronous connections.
It has support for asynchronous connections since 2.2.0.

Momoko can be installed with *easy_install* or pip_::

	pip install momoko

The latest sources can be cloned from the `Github repository`_ and
installed with::

   python setup.py install


.. _Tornado: http://www.tornadoweb.org/
.. _Psycopg2: http://initd.org/psycopg/
.. _pip: http://www.pip-installer.org/
.. _Github repository: https://github.com/FSX/momoko
