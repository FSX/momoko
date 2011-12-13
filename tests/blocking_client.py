#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import momoko
import unittest
from client_settings import client_settings

class BlockingClientTest(unittest.TestCase):
    '''
    BlockingClient tests
    '''
    def setUp(self):
        super(BlockingClientTest, self).setUp()
        self.db = momoko.BlockingClient(client_settings)        

    def tearDown(self):
        super(BlockingClientTest, self).tearDown()

    def test_single_query(self):
        '''
        Test executing a single SQL query.
        '''
        with self.db.connection as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT 42, 12, 40, 11;')
        self.assertEqual(cursor.fetchall(), [(42, 12, 40, 11)])

if __name__ == '__main__':
    unittest.main()
