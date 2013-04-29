.. _tutorial:

Tutorial
========

This tutorial will demonstrate all the functionality found in Momoko. It's assumed
a working PostgreSQL database is avaiable and dll examples are in the context of a
webaplication. Not everything is explained.\, because Momoko just wraps Psycopg2
and this means the `Psycopg2 documentation`_ must be used along with Momoko's.


Boilerplate
-----------

Here's the code that's needed for this tutorial. Each example will replace parts
or extend upon this code. The code is kept simple and minimal, its purpose is just
to demonstrate Momoko's functionality. Here goes::

    from tornado import gen
    from tornado.ioloop import IOLoop
    from tornado.httpserver import HTTPServer
    from tornado.options import parse_command_line
    from tornado.web import *

    import psycopg2
    import momoko


    class BaseHandler(RequestHandler):
        @property
        def db(self):
            return self.application.db


    class TutorialHandler(BaseHandler):
        def get(self):
            self.write('Some text here!')
            self.finish()


    if __name__ == '__main__':
        parse_command_line()
        application = Application([
            (r'/', TutorialHandler)
        ], debug=True)

        application.db = momoko.Pool(
            dsn='dbname=your_db user=your_user password=very_secret_password '
                'host=localhost port=5432',
            size=1
        )

        http_server = HTTPServer(application)
        http_server.listen(8888, 'localhost')
        IOLoop.instance().start()

For more information about all the parameters passed to ``momoko.Pool`` see
:py:class:`momoko.Pool` in the API documentation.


Usage
-----

:py:meth:`~momoko.Pool.execute`, :py:meth:`~momoko.Pool.callproc`, :py:meth:`~momoko.Pool.transaction`
and  :py:meth:`~momoko.Pool.mogrify` are methods of :py:class:`momoko.Pool` and
can  be used to query the database. Well, ``mogrify`` not really, it's used to
escape strings, but t needs a connection. All these methods, except ``mogrify``,
return a cursor or an exception object. All of the described retrieval methods in
Psycopg2's documentation like fetchone_, fetchmany_, fetchall_, etc.  can be used
to fetch the results.

All of the example will be using `tornado.gen`_ instead of callbacks, because callbacks
are fairly simple and doesn't require as much explanation. Here's one example using a
callback::

    class TutorialHandler(BaseHandler):
        @asynchronous
        def get(self):
            self.db.execute('SELECT 1;', callback=self._done)

        def _done(self, cursor, error):
            self.write('Results: %r' % (cursor.fetchall(),))
            self.finish()

The callback only need to accept two parameters. The first one is the cursor and
the second is the exception object. The exception object is ``None`` when no error
occurs and it contains an instance of one of Psycopg2's exceptions_ when an error
did occur. That's all there's to know when using callbacks.

Instead of using `tornado.gen`_ directly (or using plain callbacks) Momoko provides
subclasses of Task_, Wait_ and WaitAll_ that have some advantages. These are
:py:class:`~momoko.Op`, :py:class:`~momoko.WaitOp` and :py:class:`~momoko.WaitAllOps`.
These three classes only give a cursor back and raise an exception when something
goes wrong. Here's an example using :py:class:`~momoko.Op`::

    class TutorialHandler(BaseHandler):
        @asynchronous
        @gen.engine
        def get(self):
            try:
                cursor = yield momoko.Op(self.db.execute, 'SELECT 1;')
            except (psycopg2.Warning, psycopg2.Error) as error:
                self.write(str(error))
            else:
                self.write('Results: %r' % (cursor.fetchall(),))

            self.finish()

An example with :py:class:`~momoko.WaitOp`::

    class TutorialHandler(BaseHandler):
        @asynchronous
        @gen.engine
        def get(self):
            self.db.execute('SELECT 1;', callback=(yield gen.Callback('q1')))
            self.db.execute('SELECT 2;', callback=(yield gen.Callback('q2')))
            self.db.execute('SELECT 3;', callback=(yield gen.Callback('q3')))

            try:
                cursor1 = yield momoko.WaitOp('q1')
                cursor2 = yield momoko.WaitOp('q2')
                cursor3 = yield momoko.WaitOp('q3')
            except (psycopg2.Warning, psycopg2.Error) as error:
                self.write(str(error))
            else:
                self.write('Q1: %r<br>' % (cursor1.fetchall(),))
                self.write('Q2: %r<br>' % (cursor2.fetchall(),))
                self.write('Q3: %r<br>' % (cursor3.fetchall(),))

            self.finish()

:py:class:`~momoko.WaitAllOps` can be used instead of three separate
:py:class:`~momoko.WaitOp` calls::

    try:
        cursor1, cursor2, cursor3 = yield momoko.WaitAllOps(('q1', 'q2', 'q3'))
    except (psycopg2.Warning, psycopg2.Error) as error:
        self.write(str(error))
    else:
        self.write('Q1: %r<br>' % (cursor1.fetchall(),))
        self.write('Q2: %r<br>' % (cursor2.fetchall(),))
        self.write('Q3: %r<br>' % (cursor3.fetchall(),))

All the above examples are using :py:meth:`~momoko.Pool.execute`, but are possible
with :py:meth:`~momoko.Pool.callproc`, :py:meth:`~momoko.Pool.transaction` and
:py:meth:`~momoko.Pool.mogrify` too.


.. _Psycopg2 documentation: http://initd.org/psycopg/docs/cursor.html
.. _tornado.gen: http://www.tornadoweb.org/documentation/gen.html
.. _fetchone: http://initd.org/psycopg/docs/cursor.html#cursor.fetchone
.. _fetchmany: http://initd.org/psycopg/docs/cursor.html#cursor.fetchmany
.. _fetchall: http://initd.org/psycopg/docs/cursor.html#cursor.fetchall
.. _Task: http://www.tornadoweb.org/documentation/gen.html#tornado.gen.Task
.. _Wait: http://www.tornadoweb.org/documentation/gen.html#tornado.gen.Wait
.. _WaitAll: http://www.tornadoweb.org/documentation/gen.html#tornado.gen.WaitAll
.. _exceptions: http://initd.org/psycopg/docs/module.html#exceptions
