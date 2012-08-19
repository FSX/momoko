#!/usr/bin/env python

import sys
import unittest

import tornado.ioloop
import tornado.testing
import momoko

import settings
import psycopg2


class AsyncClientTest(tornado.testing.AsyncTestCase):
    """``AsyncClient`` tests.
    """
    def setUp(self):
        super(AsyncClientTest, self).setUp()
        self.db = momoko.AsyncClient({
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
        self.db.close()
        super(AsyncClientTest, self).tearDown()

    def test_single_query(self):
        """Test executing a single SQL query.
        """
        self.db.execute('SELECT 42, 12, 40, 11;', callback=self.stop)
        cursor = self.wait()
        self.assertEqual(cursor.fetchall(), [(42, 12, 40, 11)])

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

        self.db.batch(input, callback=self.stop)
        cursors = self.wait()

        for key, cursor in cursors.items():
            self.assertEqual(cursor.fetchall(), expected[key])

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

        self.db.chain(input, callback=self.stop)
        cursors = self.wait()

        for index, cursor in enumerate(cursors):
            self.assertEqual(cursor.fetchall(), expected[index])

    def test_transaction(self):
        """Test executing a chain query.
        """
        input = (
            ['begin;'],
            ['create local temporary table async_test on commit drop as select 42;'],
            ['select * from async_test;'],
            ['commit;'],
            ["select 1 from information_schema.tables where table_name='async_test';"]
            )
        expected = (
            None,
            None,
            [(42,)],
            None,
            []
        )

        self.db.transaction(input, callback=self.stop)
        cursors = self.wait()

        for index, cursor in enumerate(cursors):
            if expected[index] is not None:
                self.assertEqual(cursor.fetchall(), expected[index])
            else:
                self.assertRaises(psycopg2.ProgrammingError,cursor.fetchall)

if __name__ == '__main__':
    unittest.main()
