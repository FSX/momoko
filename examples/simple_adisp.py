#!/usr/bin/env python

import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web
from tornado.httpclient import AsyncHTTPClient

import momoko


class BaseHandler(tornado.web.RequestHandler):
    @property
    def db(self):
        if not hasattr(self.application, 'db'):
            self.application.db = momoko.AdispClient({
                'host': 'localhost',
                'database': 'infunadb',
                'user': 'infuna',
                'password': 'password',
                'min_conn': 1,
                'max_conn': 20,
                'cleanup_timeout': 10
            })
        return self.application.db


class MainHandler(BaseHandler):
    @tornado.web.asynchronous
    @momoko.process
    def get(self):
        cursor = yield self.db.execute('SELECT 42, 12, 40, 11;')
        self.write('Query results: %s' % cursor.fetchall())
        self.finish()


class ChainHandler(BaseHandler):
    @tornado.web.asynchronous
    @momoko.process
    def get(self):
        cursors = yield self.db.chain((
            'SELECT 22, 44, 55, 11;',
            self._chain_link_1,
            ['SELECT %s, %s, %s, %s;']
        ))
        for cursor in cursors:
            self.write('Query results: %s<br>' % cursor.fetchall())
        self.finish()

    def _chain_link_1(self, cursor):
        return [i*2 for i in cursor.fetchall()[0]]


class BatchHandler(BaseHandler):
    @tornado.web.asynchronous
    @momoko.process
    def get(self):
        cursors = yield self.db.batch([
            'SELECT 42, 12, 40, 11;',
            ['SELECT %s, %s;', (45, 14)]
        ])
        for cursor in cursors:
            self.write('Query results: %s<br>' % cursor.fetchall())
        self.finish()


def main():
    try:
        tornado.options.parse_command_line()
        application = tornado.web.Application([
            (r'/', MainHandler),
            (r'/chain', ChainHandler),
            (r'/batch', BatchHandler)
        ], debug=True)
        http_server = tornado.httpserver.HTTPServer(application)
        http_server.bind(8888)
        http_server.start(0) # Forks multiple sub-processes
        tornado.ioloop.IOLoop.instance().start()
    except KeyboardInterrupt:
        print 'Exit'


if __name__ == '__main__':
    main()
