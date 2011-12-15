#!/usr/bin/env python

import sys
import unittest

import tornado.ioloop
import tornado.testing
import momoko

import settings


class AdispClientTest(tornado.testing.AsyncTestCase):
    """``AdispClient`` tests.
    """
    def setUp(self):
        super(AdispClientTest, self).setUp()
        self.db = momoko.AdispClient({
            'host': settings.host,
            'port': settings.port,
            'database': settings.database,
            'user': settings.user,
            'password': settings.password,
            'min_conn': settings.min_conn,
            'max_conn': settings.max_conn,
            'cleanup_timeout': settings.cleanup_timeout,
            'ioloop': self.io_loop
        })

    def tearDown(self):
        super(AdispClientTest, self).tearDown()

    @momoko.process
    def test_single_query(self):
        """Test executing a single SQL query.
        """
        cursor = yield self.db.execute('SELECT 42, 12, 40, 11;')
        self.assertEqual(cursor.fetchall(), [(42, 12, 40, 11)])

    @momoko.process
    def test_batch_query(self):
        """Test executing a batch query.
        """
        input = {
            'query1': ['SELECT 42, 12, %s, %s;', (23, 56)],
            'query2': 'SELECT 1, 2, 3, 4, 5;',
            'query3': 'SELECT 465767, 4567, 3454;'
        }
        expected = {
            'query1': [(42, 12, 23, 56)],
            'query2': [(1, 2, 3, 4, 5)],
            'query3': [(465767, 4567, 3454)]
        }

        cursors = yield self.db.batch(input)

        for key, cursor in cursors.items():
            self.assertEqual(cursor.fetchall(), expected[key])

    @momoko.process
    def test_chain_query(self):
        """Test executing a chain query.
        """
        input = (
            ['SELECT 42, 12, %s, 11;', (23,)],
            'SELECT 1, 2, 3, 4, 5;'
        )
        expected = (
            [(42, 12, 23, 11)],
            [(1, 2, 3, 4, 5)]
        )

        cursors = yield self.db.chain(input)

        for index, cursor in enumerate(cursors):
            self.assertEqual(cursor.fetchall(), expected[index])


if __name__ == '__main__':
    unittest.main()
