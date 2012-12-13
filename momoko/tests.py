import os
import unittest

import momoko
from tornado.testing import AsyncTestCase


db_database = os.environ.get('MOMOKO_TEST_DB', None)
db_user = os.environ.get('MOMOKO_TEST_USER', None)
db_password = os.environ.get('MOMOKO_TEST_PASSWORD', None)
db_host = os.environ.get('MOMOKO_TEST_HOST', None)
db_port = os.environ.get('MOMOKO_TEST_PORT', None)

assert (db_database or db_user or db_password or db_host or db_port) is not None, (
    'Environment variables for the unit tests are not set. Please set the following '
    'variables: MOMOKO_TEST_DB, MOMOKO_TEST_USER, MOMOKO_TEST_PASSWORD, '
    'MOMOKO_TEST_HOST, MOMOKO_TEST_PORT')


class MomokoTest(AsyncTestCase):
    def setUp(self):
        super(MomokoTest, self).setUp()

        dsn = 'dbname=%s user=%s password=%s host=%s port=%s' % (
            db_database,
            db_user,
            db_password,
            db_host,
            db_port
        )

        self.db = momoko.Pool(
            dsn=dsn,
            minconn=1,
            maxconn=10,
            cleanup_timeout=10,
            ioloop=self.io_loop
        )

    def tearDown(self):
        self.db.close()
        super(MomokoTest, self).tearDown()

    def stop_callback(self, result, error):
        self.stop((result, error))

    def test_single_query(self):
        self.db.execute('SELECT 42, 12, 40, 11;', callback=self.stop_callback)
        cursor, error = self.wait()
        self.assertEqual(cursor.fetchall(), [(42, 12, 40, 11)])
