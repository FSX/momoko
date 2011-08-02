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


class OverviewHandler(BaseHandler):
    @tornado.web.asynchronous
    def get(self):
        self.write('''
<ul>
    <li><a href="/query">A single query</a></li>
    <li><a href="/batch">A batch of queries</a></li>
    <li><a href="/chain">A chain of queries</a></li>
</ul>
        ''')
        self.finish()


class SingleQueryHandler(BaseHandler):
    @tornado.web.asynchronous
    @momoko.process
    def get(self):
        cursor = yield self.db.execute('SELECT 42, 12, 40, 11;')
        self.write('Query results: %s' % cursor.fetchall())
        self.finish()


def main():
    try:
        tornado.options.parse_command_line()
        application = tornado.web.Application([
            (r'/', OverviewHandler),
            (r'/query', SingleQueryHandler),
            (r'/batch', BatchQueryHandler),
            (r'/chain', QueryChainHandler),
        ], debug=True)
        http_server = tornado.httpserver.HTTPServer(application)
        http_server.listen(8888)
        tornado.ioloop.IOLoop.instance().start()
    except KeyboardInterrupt:
        print('Exit')


if __name__ == '__main__':
    main()
