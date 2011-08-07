.. _examples:

Examples
========

Some examples for the blocking- and callback-style APIs. Only the relevant code
is included on this page. Go to the `examples directory`_ at the Git repository
to view the complete examples. The examples are simple enough to understand do
need an extensive manual. See the :ref:`api` for a more detailed decription of
all methods.

.. _`examples directory`: https://github.com/FSX/momoko/tree/master/examples

The differences between using the blocking-style or callback-style APIs:

* ``AdispClient`` is used to make a connection with the blocking-style API.
  For the callback-style API ``Client`` is used.
* The function in the callback-style API always need a callback function.
* The blocking-style API requires an extra decorator: ``momoko.process``.
* The blocking-style API requires the ``yield`` keyword before a function call.

Besides that both accept the same paramaters (ignoring the callback function)
and also return the same results.

Callback-style::

    class BaseHandler(tornado.web.RequestHandler):
        @property
        def db(self):
            # Create a database connection when a request handler is called
            # and store the connection in the application object.
            if not hasattr(self.application, 'db'):
                self.application.db = momoko.Client({
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

Blocking-style::

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
