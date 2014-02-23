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
good_dsn = 'dbname=%s user=%s password=%s host=%s port=%s' % (
    db_database, db_user, db_password, db_host, db_port)
bad_dsn = 'dbname=%s user=%s password=xx%s host=%s port=%s' % (
    'db', 'user', 'password', "127.0.0.127", 11111)

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

# Suspress connection errors on volatile db tests
momoko.Pool.log_connect_errors = False


class BaseTest(AsyncTestCase):
    pool_size = 3
    max_size = None
    raise_connect_errors = True
    dsn = good_dsn

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

    def build_pool(self, dsn=None, setsession=[]):
        db = momoko.Pool(
            dsn=(dsn or self.dsn),
            size=self.pool_size,
            max_size=self.max_size,
            callback=self.stop,
            ioloop=self.io_loop,
            setsession=setsession,
            raise_connect_errors=self.raise_connect_errors,
        )
        self.wait()
        return db

    def kill_connections(self, db, amount=None):
        amount = amount or len(db._conns.free)
        for conn in db._conns.free:
            if not amount:
                break
            if not conn.closed:
                conn.close()
                amount -= 1

    def run_and_check_query(self, db):
        db.execute('SELECT 6, 19, 24;', callback=self.stop_callback)
        cursor = self.wait_for_result()
        self.assert_equal(cursor.fetchall(), [(6, 19, 24)])


class MomokoBaseTest(BaseTest):

    def set_up(self):
        self.db = self.build_pool()

    def tear_down(self):
        self.db.close()


class MomokoBaseDataTest(MomokoBaseTest):
    def clean_db(self):
        self.db.execute('DROP TABLE IF EXISTS unit_test_large_query;',
                        callback=self.stop_callback)
        self.wait_for_result()
        self.db.execute('DROP TABLE IF EXISTS unit_test_transaction;',
                        callback=self.stop_callback)
        self.wait_for_result()
        self.db.execute('DROP TABLE IF EXISTS  unit_test_int_table;',
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
            'CREATE TABLE unit_test_int_table (id integer);',
            callback=self.stop_callback)
        self.wait_for_result()

        self.db.execute(
            'CREATE OR REPLACE FUNCTION unit_test_callproc(n integer)\n'
            'RETURNS integer AS $BODY$BEGIN\n'
            'RETURN n*n;\n'
            'END;$BODY$ LANGUAGE plpgsql VOLATILE;',
            callback=self.stop_callback)
        self.wait_for_result()

    def fill_int_data(self, amount=1000):
        self.db.transaction([
            "INSERT INTO unit_test_int_table VALUES %s" % ",".join("(%s)" % i for i in range(amount)),
        ], callback=self.stop_callback)
        self.wait_for_result()

    def set_up(self):
        super(MomokoBaseDataTest, self).set_up()
        self.prepare_db()

    def tear_down(self):
        self.clean_db()
        super(MomokoBaseDataTest, self).tear_down()


class MomokoTest(MomokoBaseDataTest):

    def test_single_query(self):
        """Testing single query"""
        self.run_and_check_query(self.db)

    def test_large_query(self):
        """Testing support for large queries"""
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
            """Testing hstore"""
            self.db.register_hstore(callback=self.stop_callback)
            self.wait()

            self.db.execute('SELECT \'a=>b, c=>d\'::hstore;', callback=self.stop_callback)
            cursor = self.wait_for_result()
            self.assert_equal(cursor.fetchall(), [({'a': 'b', 'c': 'd'},)])

            self.db.execute('SELECT %s;', ({'e': 'f', 'g': 'h'},), callback=self.stop_callback)
            cursor = self.wait_for_result()
            self.assert_equal(cursor.fetchall(), [({'e': 'f', 'g': 'h'},)])

    def test_callproc(self):
        """Testing callproc"""
        self.db.callproc('unit_test_callproc', (64,), callback=self.stop_callback)
        cursor = self.wait_for_result()
        self.assert_equal(cursor.fetchone(), (4096,))

    def test_query_error(self):
        """Testing that execute method propages exception properly"""
        self.db.execute('SELECT DOES NOT WORK!;', callback=self.stop_callback)
        _, error = self.wait()
        self.assert_is_instance(error, psycopg2.ProgrammingError)

    def test_mogrify(self):
        """Testing mogrify"""
        self.db.mogrify('SELECT %s, %s;', ('\'"test"\'', 'SELECT 1;'),
                        callback=self.stop_callback)
        sql = self.wait_for_result()
        self.assert_equal(sql, b'SELECT E\'\'\'"test"\'\'\', E\'SELECT 1;\';')

        self.db.execute(sql, callback=self.stop_callback)
        _, error = self.wait()
        self.assert_equal(error, None)

    def test_mogrify_error(self):
        """Testing that mogri propagates exception properly"""
        self.db.mogrify('SELECT %(foos;', {'foo': 'bar'},
                        callback=self.stop_callback)
        _, error = self.wait()
        self.assert_is_instance(error, psycopg2.ProgrammingError)

    def test_transaction(self):
        """Testing transaction functionality"""
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
        """Testing transaction auto-rollback functionality"""
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
        """Testing Op"""
        @gen.engine
        def func():
            cursor = yield momoko.Op(self.db.execute, 'SELECT 1;')
            self.assert_equal(cursor.fetchone(), (1,))
            self.stop()

        self.run_gen(func)

    def test_op_exception(self):
        """Testing that Op propagates exception properly"""
        @gen.engine
        def func():
            cursor = yield momoko.Op(self.db.execute, 'SELECT DOES NOT WORK!;')
            self.stop()

        self.assert_raises(psycopg2.ProgrammingError, self.run_gen, func)

    def test_wait_op(self):
        """Testing WaitOp"""
        @gen.engine
        def func():
            self.db.execute('SELECT 1;', callback=(yield gen.Callback('q1')))
            cursor = yield momoko.WaitOp('q1')
            self.assert_equal(cursor.fetchone(), (1,))
            self.stop()

        self.run_gen(func)

    def test_wait_op_exception(self):
        """Testing that WaitOp propagates exception properly"""
        @gen.engine
        def func():
            self.db.execute('SELECT DOES NOT WORK!;', callback=(yield gen.Callback('q1')))
            cursor = yield momoko.WaitOp('q1')
            self.stop()

        self.assert_raises(psycopg2.ProgrammingError, self.run_gen, func)

    def test_wait_all_ops(self):
        """Testing WaitAllOps"""
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
        """Testing that WaitAllOps propagates exception properly"""
        @gen.engine
        def func():
            self.db.execute('SELECT asfdsfe;', callback=(yield gen.Callback('q1')))
            self.db.execute('SELECT DOES NOT WORK!;', callback=(yield gen.Callback('q2')))
            self.db.execute('SELECT 1;', callback=(yield gen.Callback('q3')))

            cursor1, cursor2, cursor3 = yield momoko.WaitAllOps(('q1', 'q2', 'q3'))

            self.stop()

        self.assert_raises(psycopg2.ProgrammingError, self.run_gen, func)

    def test_transaction_with_reconnect(self):
        """Test whether transaction works after reconnect"""

        # Added result counting, since there was a bug in retry mechanism that caused
        # double-execution of query after reconnect
        self.kill_connections(self.db)
        self.db.transaction(("INSERT INTO unit_test_int_table VALUES (1)",),
                            callback=self.stop_callback)
        self.wait_for_result()
        self.db.execute("SELECT COUNT(1) FROM unit_test_int_table", callback=self.stop_callback)
        cursor = self.wait_for_result()
        self.assert_equal(cursor.fetchall(), [(1,)])

    def test_getconn_putconn(self):
        """Testing getconn/putconn functionality"""
        for i in range(self.pool_size * 2):
            # Run many times to check that connections get recycled properly
            self.db.getconn(callback=self.stop_callback)
            connection = self.wait_for_result()
            for j in range(10):
                connection.execute("SELECT %s", (j,), callback=self.stop_callback)
                cursor = self.wait_for_result()
                self.assert_equal(cursor.fetchall(), [(j, )])
            self.db.putconn(connection)

    def test_getconn_manage(self):
        """Testing getconn + context manager functionality"""
        for i in range(self.pool_size * 2):
            # Run many times to check that connections get recycled properly
            self.db.getconn(callback=self.stop_callback)
            connection = self.wait_for_result()
            with self.db.manage(connection):
                for j in range(10):
                    connection.execute("SELECT %s", (j,), callback=self.stop_callback)
                    cursor = self.wait_for_result()
                    self.assert_equal(cursor.fetchall(), [(j, )])


class MomokoServerSideCursorTest(MomokoBaseDataTest):
    def execute(self, connection, query, params=()):
        connection.execute(query, params, callback=self.stop_callback)
        return self.wait_for_result()

    def test_server_side_cursor(self):
        """Testing server side cursors support"""
        int_count = 1000
        offset = 0
        chunk = 10
        self.fill_int_data(int_count)

        self.db.getconn(callback=self.stop_callback)
        connection = self.wait_for_result()
        with self.db.manage(connection):
            self.execute(connection, "BEGIN")
            self.execute(connection, "DECLARE all_ints CURSOR FOR SELECT * FROM unit_test_int_table")
            while offset < int_count:
                cursor = self.execute(connection, "FETCH %s FROM all_ints", (chunk,))
                self.assert_equal(cursor.fetchall(), [(i, ) for i in range(offset, offset+chunk)])
                offset += chunk
            self.execute(connection, "CLOSE all_ints")


class MomokoParallelTest(MomokoBaseTest):
    def test_parallel_queries(self, jobs=None):
        """Testing that pool queries database in parallel"""
        sleep_time = 2

        @gen.engine
        def func():
            qnum = jobs or max(self.pool_size, self.max_size)
            for i in range(qnum):
                self.db.execute('SELECT pg_sleep(%s);' % sleep_time,
                                callback=(yield gen.Callback('q%s' % i)))

            yield momoko.WaitAllOps(["q%s" % i for i in range(qnum)])
            self.stop()

        start_time = time.time()
        self.run_gen(func)
        execution_time = time.time() - start_time
        self.assertLess(execution_time, sleep_time*1.10, msg="Query execution was too long")

    def test_parallel_queries_after_reconnect_all(self):
        """Testing that pool still queries database in parallel after ALL connections were killed"""
        self.kill_connections(self.db)
        self.test_parallel_queries()

    def test_parallel_queries_after_reconnect_some(self):
        """Testing that pool still queries database in parallel after SOME connections were killed"""
        self.kill_connections(self.db)
        self.kill_connections(self.db, amount=self.pool_size/2)
        self.test_parallel_queries()


class MomokoStretchTest(MomokoParallelTest):
    pool_size = 1
    max_size = 5

    def test_parallel_queries(self):
        """Run parallel queies and check that pool size matches number of jobs"""
        jobs = self.max_size - 1
        super(MomokoStretchTest, self).test_parallel_queries(jobs)
        self.assert_equal(self.db._conns.total, jobs)

    def test_dont_stretch(self):
        """Testing that we do not stretch unless needed"""
        self.run_and_check_query(self.db)
        self.assert_equal(self.db._conns.total, self.pool_size)

    def test_dont_stretch_after_reconnect(self):
        """Testing that reconnecting dead connection does not trigger pool stretch"""
        self.kill_connections(self.db)
        self.test_dont_stretch()

    def test_stretch_after_disonnect(self):
        """Testing that stretch works after disconnect"""
        self.kill_connections(self.db)
        self.test_parallel_queries()

    def test_stretch_genconn(self):
        """Testing that stretch works together with get/putconn"""
        @gen.engine
        def func():
            self.db.getconn(callback=(yield gen.Callback('q1')))
            self.db.getconn(callback=(yield gen.Callback('q2')))
            self.db.getconn(callback=(yield gen.Callback('q3')))

            conn1, conn2, conn3 = yield momoko.WaitAllOps(('q1', 'q2', 'q3'))

            conn1.execute('SELECT 1;', callback=(yield gen.Callback('q1')))
            conn2.execute('SELECT 2;', callback=(yield gen.Callback('q2')))
            conn3.execute('SELECT 3;', callback=(yield gen.Callback('q3')))

            cursor1, cursor2, cursor3 = yield momoko.WaitAllOps(('q1', 'q2', 'q3'))

            self.assert_equal(cursor1.fetchone(), (1,))
            self.assert_equal(cursor2.fetchone(), (2,))
            self.assert_equal(cursor3.fetchone(), (3,))

            for conn in conn1, conn2, conn3:
                self.db.putconn(conn)

            self.stop()

        self.run_gen(func)
        self.assert_equal(self.db._conns.total, 3)


class MomokoSetsessionTest(BaseTest):
    pool_size = 1

    def test_setsession(self):
        """Testing that setssion parameter is honoured"""
        setsession = deque([None, "SELECT 1", "SELECT 2"])
        time_zones = ["UTC", "Israel", "Europe/London"]

        for i in range(len(time_zones)):
            setsession[i] = "SET TIME ZONE '%s'" % time_zones[i]
            db = self.build_pool(setsession=setsession)
            db.execute("SELECT current_setting('TIMEZONE');", callback=self.stop_callback)
            cursor = self.wait_for_result()
            self.assert_equal(cursor.fetchall(), [(time_zones[i],)])
            db.close()
            setsession.rotate(1)


class MomokoVolatileDbTest(BaseTest):
    raise_connect_errors = False
    pool_size = 3

    def test_startup(self):
        """Testing that all connections are dead after pool init with bad dsn"""
        db = self.build_pool(dsn=bad_dsn)
        self.assert_equal(self.pool_size, len(db._conns.dead))

    def test_reconnect(self):
        """Testing if we can reconnect if connections die"""
        db = self.build_pool(dsn=good_dsn)
        self.kill_connections(db)
        self.run_and_check_query(db)

    def test_reconnect_interval_good_path(self):
        """Testing that we can recover if database was down during startup"""
        db = self.build_pool(dsn=bad_dsn)
        db.dsn = good_dsn
        time.sleep(db._conns.reconnect_interval)
        self.run_and_check_query(db)

    def test_reconnect_interval_bad_path(self):
        """Testing that pool does not try to reconnect right after last connection attempt failed"""
        db = self.build_pool(dsn=bad_dsn)
        try:
            self.run_and_check_query(db)
        except psycopg2.DatabaseError:
            pass


if __name__ == '__main__':
    unittest.main()
