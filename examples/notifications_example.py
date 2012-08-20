#!/usr/bin/env python

import momoko
import settings
from tornado.ioloop import IOLoop
from functools import partial


def connection_opened(connection, error):
    cursor = connection.cursor()
    cursor.execute('LISTEN test;')
    connection.set_callbacks(partial(query_callback, connection, cursor))


def query_callback(connection, cursor, error):
    connection.set_callbacks(partial(poll_callback, connection))


def poll_callback(connection, error):
    while connection._connection.notifies:
        notify = connection._connection.notifies.pop()
        print('Got NOTIFY:', notify.pid, notify.channel, notify.payload)

    connection.set_callbacks(partial(poll_callback, connection))


def main():
    try:
        dsn = 'dbname=%s user=%s password=%s host=%s port=%s' % (
            settings.database,
            settings.user,
            settings.password,
            settings.host,
            settings.port
        )

        connection = momoko.Connection(IOLoop.instance())
        connection.open(dsn=dsn, callbacks=(
            partial(connection_opened, connection),))

        IOLoop.instance().start()
    except KeyboardInterrupt:
        print('Exit')


if __name__ == '__main__':
    main()
