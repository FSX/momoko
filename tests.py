import os
import string
import random
import time
import unittest
from collections import deque

from tornado import gen
from tornado.testing import AsyncTestCase


db_database = os.environ.get('MOMOKO_TEST_DB', 'momoko_test')
db_user = os.environ.get('MOMOKO_TEST_USER', 'postgres')
db_password = os.environ.get('MOMOKO_TEST_PASSWORD', '')
db_host = os.environ.get('MOMOKO_TEST_HOST', '')
db_port = os.environ.get('MOMOKO_TEST_PORT', 5432)
test_hstore = True if os.environ.get('MOMOKO_TEST_HSTORE', False) == '1' else False
dsn = 'dbname=%s user=%s password=%s host=%s port=%s' % (
    db_database, db_user, db_password, db_host, db_port)

assert (db_database or db_user or db_password or db_host or db_port) is not None, (
    'Environment variables for the unit tests are not set. Please set the following '
    'variables: MOMOKO_TEST_DB, MOMOKO_TEST_USER, MOMOKO_TEST_PASSWORD, '
    'MOMOKO_TEST_HOST, MOMOKO_TEST_PORT')


psycopg2_impl = os.environ.get('MOMOKO_PSYCOPG2_IMPL', 'psycopg2')

if psycopg2_impl == 'psycopg2cffi':
    from psycopg2cffi import compat
    compat.register()
elif psycopg2_impl == 'psycopg2ct':
    from psycopg2ct import compat
    compat.register()


import momoko
import psycopg2


class BaseTest(AsyncTestCase):
    def __init__(self, *args, **kwargs):
        self.assert_equal = self.assertEqual
        self.assert_raises = self.assertRaises
        self.assert_is_instance = lambda object, classinfo: self.assertTrue(isinstance(object, classinfo))
        super(BaseTest, self).__init__(*args, **kwargs)

    def setUp(self):
        super(BaseTest, self).setUp()
        self.set_up()

    def tearDown(self):
        self.tear_down()
        super(BaseTest, self).tearDown()

    def set_up(self):
        pass

    def tear_down(self):
        pass

    def stop_callback(self, result, error):
        self.stop((result, error))

    def run_gen(self, func):
        func()
        self.wait()

    def wait_for_result(self):
        cursor, error = self.wait()
        if error:
            raise error
        return cursor


class MomokoBaseTest(BaseTest):
    pool_size = 3

    def clean_db(self):
        self.db.execute('DROP TABLE IF EXISTS unit_test_large_query;',
                        callback=self.stop_callback)
        self.wait_for_result()
        self.db.execute('DROP TABLE IF EXISTS unit_test_transaction;',
                        callback=self.stop_callback)
        self.wait_for_result()
        self.db.execute('DROP FUNCTION IF EXISTS  unit_test_callproc(integer);',
                        callback=self.stop_callback)
        self.wait_for_result()

    def prepare_db(self):
        self.clean_db()

        self.db.execute(
            'CREATE TABLE unit_test_large_query ('
            'id serial NOT NULL, name character varying, data text);',
            callback=self.stop_callback)
        self.wait_for_result()

        self.db.execute(
            'CREATE TABLE unit_test_transaction ('
            'id serial NOT NULL, name character varying, data text);',
            callback=self.stop_callback)
        self.wait_for_result()

        self.db.execute(
            'CREATE OR REPLACE FUNCTION unit_test_callproc(n integer)\n'
            'RETURNS integer AS $BODY$BEGIN\n'
            'RETURN n*n;\n'
            'END;$BODY$ LANGUAGE plpgsql VOLATILE;',
            callback=self.stop_callback)
        self.wait_for_result()

    def set_up(self):
        self.db = momoko.Pool(
            dsn=dsn,
            size=self.pool_size,
            callback=self.stop,
            ioloop=self.io_loop
        )
        self.wait()
        self.prepare_db()

    def tear_down(self):
        self.clean_db()
        self.db.close()


class MomokoTest(MomokoBaseTest):

    def test_single_query(self):
        self.db.execute('SELECT 6, 19, 24;', callback=self.stop_callback)
        cursor = self.wait_for_result()
        self.assert_equal(cursor.fetchall(), [(6, 19, 24)])

    def test_large_query(self):
        query_size = 100000
        chars = string.ascii_letters + string.digits + string.punctuation

        for n in range(5):
            random_data = ''.join([random.choice(chars) for i in range(query_size)])
            self.db.execute('INSERT INTO unit_test_large_query (data) VALUES (%s) '
                            'RETURNING data;', (random_data,), callback=self.stop_callback)
            cursor = self.wait_for_result()
            self.assert_equal(cursor.fetchone(), (random_data,))

        self.db.execute('SELECT COUNT(*) FROM unit_test_large_query;',
                        callback=self.stop_callback)
        cursor = self.wait_for_result()
        self.assert_equal(cursor.fetchone(), (5,))

    if test_hstore:
        def test_hstore(self):
            self.db.register_hstore(callback=self.stop_callback)
            self.wait()

            self.db.execute('SELECT \'a=>b, c=>d\'::hstore;', callback=self.stop_callback)
            cursor = self.wait_for_result()
            self.assert_equal(cursor.fetchall(), [({'a': 'b', 'c': 'd'},)])

            self.db.execute('SELECT %s;', ({'e': 'f', 'g': 'h'},), callback=self.stop_callback)
            cursor = self.wait_for_result()
            self.assert_equal(cursor.fetchall(), [({'e': 'f', 'g': 'h'},)])

    def test_callproc(self):
        self.db.callproc('unit_test_callproc', (64,), callback=self.stop_callback)
        cursor = self.wait_for_result()
        self.assert_equal(cursor.fetchone(), (4096,))

    def test_query_error(self):
        self.db.execute('SELECT DOES NOT WORK!;', callback=self.stop_callback)
        _, error = self.wait()
        self.assert_is_instance(error, psycopg2.ProgrammingError)

    def test_mogrify(self):
        self.db.mogrify('SELECT %s, %s;', ('\'"test"\'', 'SELECT 1;'),
                        callback=self.stop_callback)
        sql = self.wait_for_result()
        self.assert_equal(sql, b'SELECT E\'\'\'"test"\'\'\', E\'SELECT 1;\';')

        self.db.execute(sql, callback=self.stop_callback)
        _, error = self.wait()
        self.assert_equal(error, None)

    def test_mogrify_error(self):
        self.db.mogrify('SELECT %(foos;', {'foo': 'bar'},
                        callback=self.stop_callback)
        _, error = self.wait()
        self.assert_is_instance(error, psycopg2.ProgrammingError)

    def test_transaction(self):
        self.db.transaction((
            'SELECT 1, 2, 3, 4;',
            'SELECT 5, 6, 7, 8;',
            'SELECT 9, 10, 11, 12;',
            ('SELECT %s+10, %s+10, %s+10, %s+10;', (3, 4, 5, 6)),
            'SELECT 17, 18, 19, 20;',
            ('SELECT %s+20, %s+20, %s+20, %s+20;', (1, 2, 3, 4)),
        ), callback=self.stop_callback)
        cursors = self.wait_for_result()

        self.assert_equal(len(cursors), 6)
        self.assert_equal(cursors[0].fetchone(), (1, 2, 3, 4))
        self.assert_equal(cursors[1].fetchone(), (5, 6, 7, 8))
        self.assert_equal(cursors[2].fetchone(), (9, 10, 11, 12))
        self.assert_equal(cursors[3].fetchone(), (13, 14, 15, 16))
        self.assert_equal(cursors[4].fetchone(), (17, 18, 19, 20))
        self.assert_equal(cursors[5].fetchone(), (21, 22, 23, 24))

    def test_transaction_rollback(self):
        chars = string.ascii_letters + string.digits + string.punctuation
        data = ''.join([random.choice(chars) for i in range(100)])

        self.db.transaction((
            ('INSERT INTO unit_test_transaction (data) VALUES (%s);', (data,)),
            'SELECT DOES NOT WORK!;'
        ), callback=self.stop_callback)
        _, error = self.wait()
        self.assert_is_instance(error, psycopg2.ProgrammingError)

        self.db.execute('SELECT COUNT(*) FROM unit_test_transaction;',
                        callback=self.stop_callback)
        cursor = self.wait_for_result()
        self.assert_equal(cursor.fetchone(), (0,))

    def test_op(self):
        @gen.engine
        def func():
            cursor = yield momoko.Op(self.db.execute, 'SELECT 1;')
            self.assert_equal(cursor.fetchone(), (1,))
            self.stop()

        self.run_gen(func)

    def test_op_exception(self):
        @gen.engine
        def func():
            cursor = yield momoko.Op(self.db.execute, 'SELECT DOES NOT WORK!;')
            self.stop()

        self.assert_raises(psycopg2.ProgrammingError, self.run_gen, func)

    def test_wait_op(self):
        @gen.engine
        def func():
            self.db.execute('SELECT 1;', callback=(yield gen.Callback('q1')))
            cursor = yield momoko.WaitOp('q1')
            self.assert_equal(cursor.fetchone(), (1,))
            self.stop()

        self.run_gen(func)

    def test_wait_op_exception(self):
        @gen.engine
        def func():
            self.db.execute('SELECT DOES NOT WORK!;', callback=(yield gen.Callback('q1')))
            cursor = yield momoko.WaitOp('q1')
            self.stop()

        self.assert_raises(psycopg2.ProgrammingError, self.run_gen, func)

    def test_wait_all_ops(self):
        @gen.engine
        def func():
            self.db.execute('SELECT 1;', callback=(yield gen.Callback('q1')))
            self.db.execute('SELECT 2;', callback=(yield gen.Callback('q2')))
            self.db.execute('SELECT 3;', callback=(yield gen.Callback('q3')))

            cursor1, cursor2, cursor3 = yield momoko.WaitAllOps(('q1', 'q2', 'q3'))

            self.assert_equal(cursor1.fetchone(), (1,))
            self.assert_equal(cursor2.fetchone(), (2,))
            self.assert_equal(cursor3.fetchone(), (3,))
            self.stop()

        self.run_gen(func)

    def test_wait_all_ops_exception(self):
        @gen.engine
        def func():
            self.db.execute('SELECT asfdsfe;', callback=(yield gen.Callback('q1')))
            self.db.execute('SELECT DOES NOT WORK!;', callback=(yield gen.Callback('q2')))
            self.db.execute('SELECT 1;', callback=(yield gen.Callback('q3')))

            cursor1, cursor2, cursor3 = yield momoko.WaitAllOps(('q1', 'q2', 'q3'))

            self.stop()

        self.assert_raises(psycopg2.ProgrammingError, self.run_gen, func)

    def test_parallel_queries(self):
        sleep_time = 2

        @gen.engine
        def func():
            for i in range(self.pool_size):
                self.db.execute('SELECT pg_sleep(%s);' % sleep_time,
                                callback=(yield gen.Callback('q%s' % i)))

            yield momoko.WaitAllOps(["q%s" % i for i in range(self.pool_size)])
            self.stop()

        start_time = time.time()
        self.run_gen(func)
        execution_time = time.time() - start_time
        self.assertLess(execution_time, sleep_time*1.10, msg="Query execution was too long")


class MomokoSetsessionTest(BaseTest):
    pool_size = 1

    def build_pool(self, setsession):
        db = momoko.Pool(
            dsn=dsn,
            size=self.pool_size,
            callback=self.stop,
            ioloop=self.io_loop,
            setsession=setsession,
        )
        self.wait()
        return db

    def test_setsession(self):
        setsession = deque([None, "SELECT 1", "SELECT 2"])
        time_zones = ["UTC", "Israel", "Europe/London"]

        for i in range(len(time_zones)):
            setsession[i] = "SET TIME ZONE '%s'" % time_zones[i]
            db = self.build_pool(setsession)
            db.execute("SELECT current_setting('TIMEZONE');", callback=self.stop_callback)
            cursor = self.wait_for_result()
            self.assert_equal(cursor.fetchall(), [(time_zones[i],)])
            db.close()
            setsession.rotate(1)


if __name__ == '__main__':
    unittest.main()
