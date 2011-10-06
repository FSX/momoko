.. _examples:

Examples
========

Momoko offers a few ways to interact with the database. The first one is the
blocking client. This is just a small wrapper around Psycopg2. The other two
are asynchronous clients. The example code can be found on Github_. Only
relevant code is shown on this page.

.. _Github: https://github.com/FSX/momoko/tree/master/examples


Blocking
--------

The blocking client::

    class BaseHandler(tornado.web.RequestHandler):
        @property
        def db(self):
            # Create a database connection when a request handler is called
            # and store the connection in the application object.
            if not hasattr(self.application, 'db'):
                self.application.db = momoko.BlockingClient({
                    'host': 'localhost',
                    'database': 'momoko',
                    'user': 'frank',
                    'password': '',
                    'min_conn': 1,
                    'max_conn': 20,
                    'cleanup_timeout': 10
                })
            return self.application.db


    class SingleQueryHandler(BaseHandler):
        def get(self):
            # Besides using a with statement everyting is the same as the normal
            # Psycopg2 module
            with self.db.connection() as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT 42, 12, 40, 11;')

            self.write('Query results: %s' % cursor.fetchall())
            self.finish()

Callback
--------

The asynchronous client that uses callbacks::

    class BaseHandler(tornado.web.RequestHandler):
        @property
        def db(self):
            # Create a database connection when a request handler is called
            # and store the connection in the application object.
            if not hasattr(self.application, 'db'):
                self.application.db = momoko.AsyncClient({
                    'host': 'localhost',
                    'database': 'momoko',
                    'user': 'frank',
                    'password': '',
                    'min_conn': 1,
                    'max_conn': 20,
                    'cleanup_timeout': 10
                })
            return self.application.db


    class SingleQueryHandler(BaseHandler):
        @tornado.web.asynchronous
        def get(self):
            # One simple query
            self.db.execute('SELECT 42, 12, 40, 11;', callback=self._on_response)

        def _on_response(self, cursor):
            self.write('Query results: %s' % cursor.fetchall())
            self.finish()


    class BatchQueryHandler(BaseHandler):
        @tornado.web.asynchronous
        def get(self):
            # These queries are executed all at once and therefore they need to be
            # stored in an dictionary so you know where the resulting cursors
            # come from, because they won't arrive in the same order.
            self.db.batch({
                'query1': ['SELECT 42, 12, %s, %s;', (23, 56)],
                'query2': 'SELECT 1, 2, 3, 4, 5;',
                'query3': 'SELECT 465767, 4567, 3454;'
            }, self._on_response)

        def _on_response(self, cursors):
            for key, cursor in cursors.items():
                self.write('Query results: %s = %s<br>' % (key, cursor.fetchall()))
            self.finish()


    class QueryChainHandler(BaseHandler):
        @tornado.web.asynchronous
        def get(self):
            # Execute a list of queries in the order you specified
            self.db.chain((
                ['SELECT 42, 12, %s, 11;', (23,)],
                'SELECT 1, 2, 3, 4, 5;'
            ), self._on_response)

        def _on_response(self, cursors):
            for cursor in cursors:
                self.write('Query results: %s<br>' % cursor.fetchall())
            self.finish()


Adisp
-----

The asynchronous client that uses ``adisp`` (included in Momoko)::

    class BaseHandler(tornado.web.RequestHandler):
        @property
        def db(self):
            # Create a database connection when a request handler is called
            # and store the connection in the application object.
            if not hasattr(self.application, 'db'):
                self.application.db = momoko.AdispClient({
                    'host': 'localhost',
                    'database': 'momoko',
                    'user': 'frank',
                    'password': '',
                    'min_conn': 1,
                    'max_conn': 20,
                    'cleanup_timeout': 10
                })
            return self.application.db


    class SingleQueryHandler(BaseHandler):
        @tornado.web.asynchronous
        @momoko.process
        def get(self):
            # One simple query
            cursor = yield self.db.execute('SELECT 42, 12, 40, 11;')
            self.write('Query results: %s' % cursor.fetchall())
            self.finish()


    class BatchQueryHandler(BaseHandler):
        @tornado.web.asynchronous
        @momoko.process
        def get(self):
            # These queries are executed all at once and therefore they need to be
            # stored in an dictionary so you know where the resulting cursors
            # come from, because they won't arrive in the same order.
            cursors = yield self.db.batch({
                'query1': ['SELECT 42, 12, %s, %s;', (23, 56)],
                'query2': 'SELECT 1, 2, 3, 4, 5;',
                'query3': 'SELECT 465767, 4567, 3454;'
            })
            for key, cursor in cursors.items():
                self.write('Query results: %s = %s<br>' % (key, cursor.fetchall()))
            self.finish()


    class QueryChainHandler(BaseHandler):
        @tornado.web.asynchronous
        @momoko.process
        def get(self):
            # Execute a list of queries in the order you specified
            cursors = yield self.db.chain((
                ['SELECT 42, 12, %s, 11;', (23,)],
                'SELECT 1, 2, 3, 4, 5;'
            ))
            for cursor in cursors:
                self.write('Query results: %s<br>' % cursor.fetchall())
            self.finish()


Tornado's gen
-------------

And the asynchronous callback-based client using Tornado's gen_ module::

    from tornado import gen

    class BaseHandler(tornado.web.RequestHandler):
        @property
        def db(self):
            # Create a database connection when a request handler is called
            # and store the connection in the application object.
            if not hasattr(self.application, 'db'):
                self.application.db = momoko.AsyncClient({
                    'host': 'localhost',
                    'database': 'momoko',
                    'user': 'frank',
                    'password': '',
                    'min_conn': 1,
                    'max_conn': 20,
                    'cleanup_timeout': 10
                })
            return self.application.db


    class SingleQueryHandler(BaseHandler):
        @tornado.web.asynchronous
        @gen.engine
        def get(self):
            # One simple query
            cursor = yield gen.Task(self.db.execute, 'SELECT 42, 12, %s, 11;', (25,))
            self.write('Query results: %s' % cursor.fetchall())
            self.finish()


    class BatchQueryHandler(BaseHandler):
        @tornado.web.asynchronous
        @gen.engine
        def get(self):
            # These queries are executed all at once and therefore they need to be
            # stored in an dictionary so you know where the resulting cursors
            # come from, because they won't arrive in the same order.
            cursors = yield gen.Task(self.db.batch, {
                'query1': ['SELECT 42, 12, %s, %s;', (23, 56)],
                'query2': 'SELECT 1, 2, 3, 4, 5;',
                'query3': 'SELECT 465767, 4567, 3454;'
            })

            for key, cursor in cursors.items():
                self.write('Query results: %s = %s<br>' % (key, cursor.fetchall()))
            self.finish()


    class QueryChainHandler(BaseHandler):
        @tornado.web.asynchronous
        @gen.engine
        def get(self):
            # Execute a list of queries in the order you specified
            cursors = yield gen.Task(self.db.chain, (
                ['SELECT 42, 12, %s, 11;', (23,)],
                'SELECT 1, 2, 3, 4, 5;'
            ))

            for cursor in cursors:
                self.write('Query results: %s<br>' % cursor.fetchall())
            self.finish()


    class MultiQueryHandler(BaseHandler):
        @tornado.web.asynchronous
        @gen.engine
        def get(self):
            cursor1, cursor2, cursor3 = yield [
                gen.Task(self.db.execute, 'SELECT 42, 12, %s, 11;', (25,)),
                gen.Task(self.db.execute, 'SELECT 42, 12, %s, %s;', (23, 56)),
                gen.Task(self.db.execute, 'SELECT 465767, 4567, 3454;')
            ]

            self.write('Query 1 results: %s<br>' % cursor1.fetchall())
            self.write('Query 2 results: %s<br>' % cursor2.fetchall())
            self.write('Query 3 results: %s' % cursor3.fetchall())

            self.finish()


    class CallbackWaitHandler(BaseHandler):
        @tornado.web.asynchronous
        @gen.engine
        def get(self):

            self.db.execute('SELECT 42, 12, %s, 11;', (25,),
                callback=(yield gen.Callback('q1')))
            self.db.execute('SELECT 42, 12, %s, %s;', (23, 56),
                callback=(yield gen.Callback('q2')))
            self.db.execute('SELECT 465767, 4567, 3454;',
                callback=(yield gen.Callback('q3')))

            cursor1 = yield gen.Wait('q1')
            cursor2 = yield gen.Wait('q2')
            cursor3 = yield gen.Wait('q3')

            self.write('Query 1 results: %s<br>' % cursor1.fetchall())
            self.write('Query 2 results: %s<br>' % cursor2.fetchall())
            self.write('Query 3 results: %s' % cursor3.fetchall())

            self.finish()


.. _gen: http://www.tornadoweb.org/documentation/gen.html
