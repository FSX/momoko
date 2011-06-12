#!/usr/bin/env python

import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web

import momoko


class BaseHandler(tornado.web.RequestHandler):
    @property
    def db(self):
        if not hasattr(self.application, 'db'):
            self.application.db = momoko.Client({
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
    def get(self):
        self.db.execute('SELECT 42, 12, 40, 11;', callback=self._on_response)

    def _on_response(self, cursor):
        self.write('Query results: %s' % cursor.fetchall())
        self.finish()


def main():
    try:
        tornado.options.parse_command_line()
        application = tornado.web.Application([
            (r'/', MainHandler),
        ])
        http_server = tornado.httpserver.HTTPServer(application)
        http_server.bind(8888)
        http_server.start(0) # Forks multiple sub-processes
        tornado.ioloop.IOLoop.instance().start()
    except KeyboardInterrupt:
        print('Exit')


if __name__ == '__main__':
    main()
