#!/usr/bin/env python

import momoko
import settings
from tornado.ioloop import IOLoop


def notify_callback(notify):
    print(notify)


def main():
    try:
        dsn = 'dbname=%s user=%s password=%s host=%s port=%s' % (
            settings.database,
            settings.user,
            settings.password,
            settings.host,
            settings.port
        )

        connection = momoko.Connection('test', notify_callback, IOLoop.instance())
        connection.open(dsn=dsn)

        IOLoop.instance().start()
    except KeyboardInterrupt:
        print('Exit')


if __name__ == '__main__':
    main()
