#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import momoko
import tornado.ioloop
import tornado.testing
import unittest

class AsyncClientTest(tornado.testing.AsyncTestCase):
    '''
    AsyncClient tests
    '''
    def setUp(self):
        super(AsyncClientTest, self).setUp()
        
        self.db = momoko.AsyncClient({
                'host': 'localhost',
                'database': 'momoko',
                'user': 'frank',
                'password': '',
                'min_conn': 1,
                'max_conn': 20,
                'cleanup_timeout': 10,
                'ioloop': self.io_loop})

    def tearDown(self):
        super(AsyncClientTest, self).tearDown()

    def test_single_query(self):
        '''
        Test executing a single SQL query.
        '''
        self.db.execute('SELECT 42, 12, 40, 11;', callback=self.stop)
        cursor = self.wait()
        self.assertEqual(cursor.fetchall(), [(42, 12, 40, 11)])

    def test_batch_query(self):
        '''
        Test executing a batch query.
        '''
        input = {
            'query1': ['SELECT 42, 12, %s, %s;', (23, 56)],
            'query2': 'SELECT 1, 2, 3, 4, 5;',
            'query3': 'SELECT 465767, 4567, 3454;'}
        expected = {
            'query1': [(42, 12, 23, 56)],
            'query2': [(1, 2, 3, 4, 5)],
            'query3': [(465767, 4567, 3454)]}
        self.db.batch(input, callback=self.stop)
        cursors = self.wait()
        for key, cursor in cursors.items():
            self.assertEqual(cursor.fetchall(), expected[key])

    def test_chain_query(self):
        '''
        Test executing a chain query.
        '''
        input = (
            ['SELECT 42, 12, %s, 11;', (23,)],
            'SELECT 1, 2, 3, 4, 5;'
            )
        expected = [[(42, 12, 23, 11)],
                    [(1, 2, 3, 4, 5)]]
        self.db.chain(input, callback=self.stop)
        cursors = self.wait()
        for index, cursor in enumerate(cursors):
            self.assertEqual(cursor.fetchall(), expected[index])

if __name__ == '__main__':
    unittest.main()
