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
        self.db.chain([
            ['SELECT 42, 12, %s, 11;', (23,)],
            self._after_first_query,
            self._after_first_callable,
            ['SELECT 1, 2, 3, 4, 5;'],
            self._before_last_query,
            ['SELECT %s, %s, %s, %s, %s;'],
            self._on_response
        ]).run()

    def _after_first_query(self, cursor):
        results = cursor.fetchall()
        return {
            'p1': results[0][0],
            'p2': results[0][1],
            'p3': results[0][2],
            'p4': results[0][3]
        }

    def _after_first_callable(self, p1, p2, p3, p4):
        self.write('Results of the first query in the chain: %s, %s, %s, %s<br>' % \
            (p1, p2, p3, p4))

    def _before_last_query(self, cursor):
        results = cursor.fetchall()
        return [i*16 for i in results[0]]

    def _on_response(self, cursor):
        self.write('Results of the last query in the chain: %s' % \
            cursor.fetchall())
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
