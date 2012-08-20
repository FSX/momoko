#!/usr/bin/env python

"""
This example uses Tornado's gen_.

.. _gen: http://www.tornadoweb.org/documentation/gen.html
"""


import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web
from tornado import gen

import momoko
import settings


class BaseHandler(tornado.web.RequestHandler):
    @property
    def db(self):
        return self.application.db


class OverviewHandler(BaseHandler):
    def get(self):
        self.write('''
<ul>
    <li><a href="/query">A single query</a></li>
    <li><a href="/multi_query">Multiple queries executed with gen.Task</a></li>
    <li><a href="/callback_and_wait">Multiple queries executed with gen.Callback and gen.Wait</a></li>
</ul>
        ''')
        self.finish()


class SingleQueryHandler(BaseHandler):
    @tornado.web.asynchronous
    @gen.engine
    def get(self):
        try:
            cursor1 = yield momoko.Op(self.db.execute, 'SELECT 55, 18, %s, 231;', (87,))
            self.write('Query results: %s<br>' % cursor1.fetchall())
        except Exception as error:
            self.write(error)

        self.finish()

    # @tornado.web.asynchronous
    # @gen.engine
    # def get(self):
    #     try:
    #         sql = yield momoko.Op(self.db.mogrify, 'SELECT 55, 18, %s, 231;', (87,))
    #         self.write('SQL: %s<br>' % sql)
    #     except Exception as error:
    #         self.write(error)

    #     self.finish()

    # @tornado.web.asynchronous
    # @gen.engine
    # def get(self):
    #     try:
    #         cursor1 = yield momoko.Op(self.db.callproc, 'insert_location', ('test_location_momoko',))
    #         self.write('Query results: %s<br>' % cursor1.fetchall())
    #     except Exception as error:
    #         self.write(str(error))

    #     self.finish()

    # @tornado.web.asynchronous
    # def get(self):
    #     self.db.execute('SELECT 42, 12, %s, 11;', (25,), callback=self._done)
    #     # self.db.execute('SELECT X;', callback=self._done)

    # def _done(self, cursor, error):
    #     if error is None:
    #         self.write('Query results: %s' % cursor.fetchall())
    #     else:
    #         self.write('Error: %r' % error)

    #     self.finish()


class MultiQueryHandler(BaseHandler):
    @tornado.web.asynchronous
    @gen.engine
    def get(self):
        cursor1, cursor2, cursor3 = yield [
            momoko.Op(self.db.execute, 'SELECT 42, 12, %s, 11;', (25,)),
            momoko.Op(self.db.execute, 'SELECT 42, 12, %s, %s;', (23, 56)),
            momoko.Op(self.db.execute, 'SELECT 465767, 4567, 3454;')
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

        # cursor1 = yield momoko.WaitOp('q1')
        # cursor2 = yield momoko.WaitOp('q2')
        # cursor3 = yield momoko.WaitOp('q3')

        cursor1, cursor2, cursor3 = yield momoko.WaitAllOps(('q1', 'q2', 'q3'))

        self.write('Query 1 results: %s<br>' % cursor1.fetchall())
        self.write('Query 2 results: %s<br>' % cursor2.fetchall())
        self.write('Query 3 results: %s' % cursor3.fetchall())

        self.finish()


def main():
    try:
        tornado.options.parse_command_line()
        application = tornado.web.Application([
            (r'/', OverviewHandler),
            (r'/query', SingleQueryHandler),
            (r'/multi_query', MultiQueryHandler),
            (r'/callback_and_wait', CallbackWaitHandler),
        ], debug=True)

        dsn = 'dbname=%s user=%s password=%s host=%s port=%s' % (
            settings.database,
            settings.user,
            settings.password,
            settings.host,
            settings.port
        )

        application.db = momoko.ConnectionPool(
            dsn=dsn,
            minconn=settings.min_conn,
            maxconn=settings.max_conn,
            cleanup_timeout=settings.cleanup_timeout
        )

        http_server = tornado.httpserver.HTTPServer(application)
        http_server.listen(8888)
        tornado.ioloop.IOLoop.instance().start()
    except KeyboardInterrupt:
        print('Exit')


if __name__ == '__main__':
    main()
