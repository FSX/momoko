#!/usr/bin/env python

"""
This example uses Tornado's gen_.

.. _gen: http://www.tornadoweb.org/documentation/gen.html
"""

import os

import tornado.web
import tornado.ioloop
import tornado.options
from tornado import gen
import tornado.httpserver

import momoko


db_database = os.environ.get('MOMOKO_TEST_DB', 'momoko_test')
db_user = os.environ.get('MOMOKO_TEST_USER', 'postgres')
db_password = os.environ.get('MOMOKO_TEST_PASSWORD', '')
db_host = os.environ.get('MOMOKO_TEST_HOST', '')
db_port = os.environ.get('MOMOKO_TEST_PORT', 5432)
enable_hstore = True if os.environ.get('MOMOKO_TEST_HSTORE', False) == '1' else False
dsn = 'dbname=%s user=%s password=%s host=%s port=%s' % (
    db_database, db_user, db_password, db_host, db_port)

assert (db_database or db_user or db_password or db_host or db_port) is not None, (
    'Environment variables for the examples are not set. Please set the following '
    'variables: MOMOKO_TEST_DB, MOMOKO_TEST_USER, MOMOKO_TEST_PASSWORD, '
    'MOMOKO_TEST_HOST, MOMOKO_TEST_PORT')


class BaseHandler(tornado.web.RequestHandler):
    @property
    def db(self):
        return self.application.db


class OverviewHandler(BaseHandler):
    def get(self):
        self.write('''
<ul>
    <li><a href="/mogrify">Mogrify</a></li>
    <li><a href="/query">A single query</a></li>
    <li><a href="/hstore">An hstore query</a></li>
    <li><a href="/transaction">A transaction</a></li>
    <li><a href="/multi_query">Multiple queries executed with gen.Task</a></li>
    <li><a href="/callback_and_wait">Multiple queries executed with gen.Callback and gen.Wait</a></li>
</ul>
        ''')
        self.finish()


class MogrifyHandler(BaseHandler):
    @tornado.web.asynchronous
    @gen.engine
    def get(self):
        try:
            sql = yield momoko.Op(self.db.mogrify, 'SELECT %s;', (1,))
            self.write('SQL: %s<br>' % sql)
        except Exception as error:
            self.write(str(error))

        self.finish()


class SingleQueryHandler(BaseHandler):
    @tornado.web.asynchronous
    @gen.engine
    def get(self):
        try:
            cursor = yield momoko.Op(self.db.execute, 'SELECT %s;', (1,))
            self.write('Query results: %s<br>' % cursor.fetchall())
        except Exception as error:
            self.write(str(error))

        self.finish()


class HstoreQueryHandler(BaseHandler):
    @tornado.web.asynchronous
    @gen.engine
    def get(self):
        if enable_hstore:
            try:
                cursor = yield momoko.Op(self.db.execute, "SELECT 'a=>b, c=>d'::hstore;")
                self.write('Query results: %s<br>' % cursor.fetchall())
                cursor = yield momoko.Op(self.db.execute, "SELECT %s;",
                    ({'e': 'f', 'g': 'h'},))
                self.write('Query results: %s<br>' % cursor.fetchall())
            except Exception as error:
                self.write(str(error))
        else:
            self.write('hstore is not enabled')

        self.finish()


class MultiQueryHandler(BaseHandler):
    @tornado.web.asynchronous
    @gen.engine
    def get(self):
        cursor1, cursor2, cursor3 = yield [
            momoko.Op(self.db.execute, 'SELECT 1;'),
            momoko.Op(self.db.mogrify, 'SELECT 2;'),
            momoko.Op(self.db.execute, 'SELECT %s;', (3*1,))
        ]

        self.write('Query 1 results: %s<br>' % cursor1.fetchall())
        self.write('Query 2 results: %s<br>' % cursor2)
        self.write('Query 3 results: %s' % cursor3.fetchall())

        self.finish()


class TransactionHandler(BaseHandler):
    @tornado.web.asynchronous
    @gen.engine
    def get(self):
        try:
            cursors = yield momoko.Op(self.db.transaction, (
                'SELECT 1, 12, 22, 11;',
                'SELECT 55, 22, 78, 13;',
                'SELECT 34, 13, 12, 34;',
                'SELECT 23, 12, 22, 23;',
                'SELECT 42, 23, 22, 11;',
                ('SELECT 49, %s, 23, 11;', ('STR',)),
            ))

            for i, cursor in enumerate(cursors):
                self.write('Query %s results: %s<br>' % (i, cursor.fetchall()))
        except Exception as error:
            self.write(str(error))

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

        # Separately...
        # cursor1 = yield momoko.WaitOp('q1')
        # cursor2 = yield momoko.WaitOp('q2')
        # cursor3 = yield momoko.WaitOp('q3')

        # Or all at once
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
            (r'/mogrify', MogrifyHandler),
            (r'/query', SingleQueryHandler),
            (r'/hstore', HstoreQueryHandler),
            (r'/transaction', TransactionHandler),
            (r'/multi_query', MultiQueryHandler),
            (r'/callback_and_wait', CallbackWaitHandler),
        ], debug=True)

        application.db = momoko.Pool(
            dsn=dsn,
            size=1
        )

        if enable_hstore:
            application.db.register_hstore()

        http_server = tornado.httpserver.HTTPServer(application)
        http_server.listen(8888, 'localhost')
        tornado.ioloop.IOLoop.instance().start()
    except KeyboardInterrupt:
        print('Exit')


if __name__ == '__main__':
    main()
