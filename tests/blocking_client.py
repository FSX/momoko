#!/usr/bin/env python

import unittest

import momoko
import settings


class BlockingClientTest(unittest.TestCase):
    """``BlockingClient`` tests.
    """
    def setUp(self):
        super(BlockingClientTest, self).setUp()
        self.db = momoko.BlockingClient({
            'host': settings.host,
            'port': settings.port,
            'database': settings.database,
            'user': settings.user,
            'password': settings.password,
            'min_conn': settings.min_conn,
            'max_conn': settings.max_conn,
            'cleanup_timeout': settings.cleanup_timeout
        })

    def tearDown(self):
        super(BlockingClientTest, self).tearDown()

    def test_single_query(self):
        """Test executing a single SQL query.
        """
        with self.db.connection as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT 42, 12, 40, 11;')

        self.assertEqual(cursor.fetchall(), [(42, 12, 40, 11)])


if __name__ == '__main__':
    unittest.main()
