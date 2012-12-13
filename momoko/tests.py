import os
import string
import random
import unittest

import momoko
from tornado.testing import AsyncTestCase


db_database = os.environ.get('MOMOKO_TEST_DB', None)
db_user = os.environ.get('MOMOKO_TEST_USER', None)
db_password = os.environ.get('MOMOKO_TEST_PASSWORD', None)
db_host = os.environ.get('MOMOKO_TEST_HOST', None)
db_port = os.environ.get('MOMOKO_TEST_PORT', None)
dsn = 'dbname=%s user=%s password=%s host=%s port=%s' % (
    db_database, db_user, db_password, db_host, db_port)

assert (db_database or db_user or db_password or db_host or db_port) is not None, (
    'Environment variables for the unit tests are not set. Please set the following '
    'variables: MOMOKO_TEST_DB, MOMOKO_TEST_USER, MOMOKO_TEST_PASSWORD, '
    'MOMOKO_TEST_HOST, MOMOKO_TEST_PORT')


class MomokoTest(AsyncTestCase):
    def stop_callback(self, result, error):
        self.stop((result, error))

    def setUp(self):
        super(MomokoTest, self).setUp()

        self.db = momoko.Pool(
            dsn=dsn,
            minconn=1,
            maxconn=10,
            cleanup_timeout=10,
            ioloop=self.io_loop
        )

    def tearDown(self):
        # Clean up if that didn't happen already
        self.db.execute('DROP TABLE IF EXISTS unit_tests;', callback=self.stop_callback)
        self.wait()
        self.db.execute('DROP FUNCTION IF EXISTS  unit_test_callproc(integer);',
            callback=self.stop_callback)
        self.wait()

        self.db.close()
        super(MomokoTest, self).tearDown()

    def test_single_query(self):
        self.db.execute('SELECT 6, 19, 24;', callback=self.stop_callback)
        cursor, error = self.wait()
        self.assertEqual(cursor.fetchall(), [(6, 19, 24)])

    def test_large_query(self):
        query_size = 100000
        chars = string.ascii_letters + string.digits + string.punctuation

        self.db.execute(
            'CREATE TABLE unit_tests ('
            'id serial NOT NULL, name character varying, data text, '
            'CONSTRAINT unit_tests_id_pk PRIMARY KEY (id));',
            callback=self.stop_callback)
        self.wait()

        for n in range(10):
            to_be_inserted = ''.join( [random.choice(chars) for i in range(query_size)])
            self.db.execute('INSERT INTO unit_tests (data) VALUES (%s) RETURNING id;',
                (to_be_inserted,), callback=self.stop_callback)
            cursor, error = self.wait()
            self.assertEqual(cursor.fetchone(), (n+1,))

        self.db.execute('SELECT COUNT(*) FROM unit_tests;', callback=self.stop_callback)
        cursor, error = self.wait()
        self.assertEqual(cursor.fetchone(), (10,))

        self.db.execute('DROP TABLE unit_tests;', callback=self.stop_callback)
        self.wait()

    def test_callproc(self):
        query_size = 100000
        chars = string.ascii_letters + string.digits + string.punctuation

        self.db.execute(
            'CREATE OR REPLACE FUNCTION unit_test_callproc(n integer)\n'
            'RETURNS integer AS $BODY$BEGIN\n'
            'RETURN n*n;\n'
            'END;$BODY$ LANGUAGE plpgsql VOLATILE;',
            callback=self.stop_callback)
        cursor, error = self.wait()

        self.db.callproc('unit_test_callproc', (64,), callback=self.stop_callback)
        cursor, error = self.wait()
        self.assertEqual(cursor.fetchone(), (4096,))

        # Destroy test table
        self.db.execute('DROP FUNCTION unit_test_callproc(integer);', callback=self.stop_callback)
        self.wait()
