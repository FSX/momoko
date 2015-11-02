.. _tutorial:

Tutorial
========

This tutorial will demonstrate all the functionality found in Momoko. It's assumed a
working PostgreSQL database is available, and everything is done in the context of a
simple tornado web application. Not everything is explained: because Momoko just
wraps Psycopg2, the `Psycopg2 documentation`_ must be used alongside Momoko's.


The principle
-------------
Almost every method of :py:meth:`~momoko.Pool` and :py:meth:`~momoko.Connection`
returns a `future`_. There are some notable exceptions, like
:py:meth:`~momoko.Pool.close`; be sure to consult API documentation for the
details.

These future objects can be simply ``yield``-ed in Tornado methods decorated with ``gen.coroutine``.
For SQL execution related methods these futures resolve to corresponding cursor objects.

Trival example
--------------
Here is the simplest synchronous version of connect/select code::

    import psycopg2
    conn = psycopg2.connect(dsn="...")
    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    rows = cursor.fetchall()

And this is how the same code looks with Momoko/Tornado::

    import momoko
    from tornado.ioloop import IOLoop
    ioloop = IOLoop.instance()

    conn = momoko.Connection(dsn="...")
    future = ioloop.add_future(conn.connect(), lambda x: ioloop.stop())
    ioloop.start()
    future.result()  # raises exception on connection error

    future = conn.execute("SELECT 1")
    ioloop.add_future(future, lambda x: ioloop.stop())
    ioloop.start()
    cursor = future.result()
    rows = cursor.fetchall()

We create connection object. Then invoke ``connect()`` method that returns future that
resolves to connection object itself when connection is ready (we already have connection
object at hand, thus we just wait until future is ready, ignoring its result).

Next we call ``execute()`` which returns future that resolves to ready-to-use cursor object.
And we use IOLoop again to wait for this future to be ready.

Now you know to use :py:meth:`~momoko.Connection` for working with with stand-alone
connections to PostgreSQL in asynchronous mode.

Introducing Pool
----------------
The real power of Momoko comes with :py:meth:`~momoko.Pool`. It provides several
nice features that make it useful in production environments:

Connection pooling
   It manages several connections and distributes queries requests between them.
   If all connections are busy, outstanding query requests are waiting in queue
Automatic pool growing (stretching)
   You can allow automatic stretching - i.e. if all connections are busy and more
   requests are coming, Pool will open more connections up a certain limit
Automatic reconnects
   If connections get terminated (database server restart, etc) Pool will automatically
   reconnect them and transparently retry query if it failed due to dead connection.


Boilerplate
^^^^^^^^^^^

Here's the code that's needed for the rest of this tutorial. Each example will replace parts
or extend upon this code. The code is kept simple and minimal; its purpose is just
to demonstrate Momoko's functionality. Here it goes::

    from tornado import gen
    from tornado.ioloop import IOLoop
    from tornado.httpserver import HTTPServer
    from tornado.options import parse_command_line
    from tornado import web

    import psycopg2
    import momoko


    class BaseHandler(web.RequestHandler):
        @property
        def db(self):
            return self.application.db


    class TutorialHandler(BaseHandler):
        def get(self):
            self.write('Some text here!')
            self.finish()


    if __name__ == '__main__':
        parse_command_line()
        application = web.Application([
            (r'/', TutorialHandler)
        ], debug=True)

        ioloop = IOLoop.instance()

        application.db = momoko.Pool(
            dsn='dbname=your_db user=your_user password=very_secret_password '
                'host=localhost port=5432',
            size=1,
            ioloop=ioloop,
        )

        # this is a one way to run ioloop in sync
        future = application.db.connect()
        ioloop.add_future(future, lambda f: ioloop.stop())
        ioloop.start()
        future.result()  # raises exception on connection error

        http_server = HTTPServer(application)
        http_server.listen(8888, 'localhost')
        ioloop.start()

For more information about all the parameters passed to ``momoko.Pool`` see
:py:class:`momoko.Pool` in the API documentation.


Using Pool
----------

:py:meth:`~momoko.Pool.execute`, :py:meth:`~momoko.Pool.callproc`, :py:meth:`~momoko.Pool.transaction`
and  :py:meth:`~momoko.Pool.mogrify` are methods of :py:class:`momoko.Pool` which
can be used to query the database. (Actually, ``mogrify()`` is only used to
escape strings, but it needs a connection). All these methods, except ``mogrify()``,
return a cursor or an exception object. All of the described retrieval methods in
Psycopg2's documentation — fetchone_, fetchmany_, fetchall_, etc. — can be used
to fetch the results.

First, lets rewrite our trivial example using Tornado web handlers::

    class TutorialHandler(BaseHandler):
        @gen.coroutine
        def get(self):
            cursor = yield self.db.execute("SELECT 1;")
            self.write("Results: %s" % cursor.fetchone())
            self.finish()

To execute several queries in parallel, accumulate corresponding futures and
yield them at once::

    class TutorialHandler(BaseHandler):
        @gen.coroutine
        def get(self):
            try:
                f1 = self.db.execute('select 1;')
                f2 = self.db.execute('select 2;')
                f3 = self.db.execute('select 3;')
                yield [f1, f2, f3]

                cursor1 = f1.result()
                cursor2 = f2.result()
                cursor3 = f3.result()

            except (psycopg2.Warning, psycopg2.Error) as error:
                self.write(str(error))
            else:
                self.write('Q1: %r<br>' % (cursor1.fetchall(),))
                self.write('Q2: %r<br>' % (cursor2.fetchall(),))
                self.write('Q3: %r<br>' % (cursor3.fetchall(),))

            self.finish()

All the above examples use :py:meth:`~momoko.Pool.execute`, but work
with :py:meth:`~momoko.Pool.callproc`, :py:meth:`~momoko.Pool.transaction` and
:py:meth:`~momoko.Pool.mogrify` too.


Advanced
--------

Manual connection management
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
You can manually acquire connection from the pool using the :py:meth:`~momoko.Pool.getconn` method.
This is very useful, for example, for server-side cursors.

It important to return connection back to the pool once you've done with it, even if an error occurs
in the middle of your work. Use either
:py:meth:`~momoko.Pool.putconn`
method or
:py:meth:`~momoko.Pool.manage`
manager to return the connection.

Here is the server-side cursor example (based on the code in momoko unittests)::

    @gen.coroutine
    def get(self):
        int_count = 1000
        offset = 0
        chunk = 10
        try:
            conn = yield self.db.getconn()
            with self.db.manage(conn):
                yield conn.execute("BEGIN")
                yield conn.execute("DECLARE all_ints CURSOR FOR SELECT * FROM unit_test_int_table")
                while offset < int_count:
                    cursor = yield conn.execute("FETCH %s FROM all_ints", (chunk,))
                    rows = cursor.fetchall()
                    # Do something with results...
                    offset += chunk
                yield conn.execute("CLOSE all_ints")
                yield conn.execute("COMMIT")

        except Exception as error:
            self.write(str(error))

.. _Psycopg2 documentation: http://initd.org/psycopg/docs/cursor.html
.. _tornado.gen: http://tornado.readthedocs.org/en/stable/gen.html
.. _fetchone: http://initd.org/psycopg/docs/cursor.html#cursor.fetchone
.. _fetchmany: http://initd.org/psycopg/docs/cursor.html#cursor.fetchmany
.. _fetchall: http://initd.org/psycopg/docs/cursor.html#cursor.fetchall
.. _Task: http://tornado.readthedocs.org/en/stable/gen.html#tornado.gen.Task
.. _Wait: http://tornado.readthedocs.org/en/stable/gen.html#tornado.gen.Wait
.. _WaitAll: http://tornado.readthedocs.org/en/stable/gen.html#tornado.gen.WaitAll
.. _exceptions: http://initd.org/psycopg/docs/module.html#exceptions
.. _future: http://tornado.readthedocs.org/en/latest/concurrent.html
