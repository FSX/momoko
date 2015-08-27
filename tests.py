from __future__ import print_function

import os
import string
import random
import time
from collections import deque
from itertools import chain
import inspect
import logging
import datetime
import threading
import socket
from tornado.concurrent import Future
import subprocess

from tornado import gen
from tornado.testing import unittest, AsyncTestCase, gen_test

import sys
if sys.version_info[0] >= 3:
    unicode = str

log = logging.getLogger("unittest")

debug = os.environ.get('MOMOKO_TEST_DEBUG', None)
db_database = os.environ.get('MOMOKO_TEST_DB', 'momoko_test')
db_user = os.environ.get('MOMOKO_TEST_USER', 'postgres')
db_password = os.environ.get('MOMOKO_TEST_PASSWORD', '')
db_host = os.environ.get('MOMOKO_TEST_HOST', '127.0.0.1')
db_port = os.environ.get('MOMOKO_TEST_PORT', 5432)
db_proxy_port = os.environ.get('MOMOKO_TEST_PROXY_PORT', 15432)
test_hstore = True if os.environ.get('MOMOKO_TEST_HSTORE', False) == '1' else False
good_dsn = 'dbname=%s user=%s password=%s host=%s port=%s' % (
    db_database, db_user, db_password, db_host, db_port)
good_proxy_dsn = 'dbname=%s user=%s password=%s hostaddr=127.0.0.1 port=%s' % (
    db_database, db_user, db_password, db_proxy_port)
bad_dsn = 'dbname=%s user=%s password=xx%s host=%s port=%s' % (
    'db', 'user', 'password', "127.0.0.127", 11111)
local_bad_dsn = 'dbname=%s user=%s password=xx%s' % (
    'db', 'user', 'password')

TCPPROXY_PATH = os.environ.get('MOMOKO_TCPPROXY_PATH', "./tcproxy/src/tcproxy")

assert (db_database or db_user or db_password or db_host or db_port) is not None, (
    'Environment variables for the unit tests are not set. Please set the following '
    'variables: MOMOKO_TEST_DB, MOMOKO_TEST_USER, MOMOKO_TEST_PASSWORD, '
    'MOMOKO_TEST_HOST, MOMOKO_TEST_PORT')

print("good_dsn %s" % good_dsn)
print("good_proxy_dsn %s" % good_proxy_dsn)
print("bad_dsn %s" % bad_dsn)
print("local_bad_dsn %s" % local_bad_dsn)

psycopg2_impl = os.environ.get('MOMOKO_PSYCOPG2_IMPL', 'psycopg2')

if psycopg2_impl == 'psycopg2cffi':
    from psycopg2cffi import compat
    compat.register()
elif psycopg2_impl == 'psycopg2ct':
    from psycopg2ct import compat
    compat.register()


import momoko
import momoko.exceptions
import psycopg2
from psycopg2.extras import RealDictConnection, RealDictCursor, NamedTupleCursor

# Suspress connection errors on volatile db tests
momoko.Pool.log_connect_errors = False


class BaseTest(AsyncTestCase):
    dsn = good_dsn
    good_dsn = good_dsn

    # This is a hack to overcome lack of "yield from" in Python < 3.3.
    # The goal is to support several set_up methods in inheriatnace chain
    # So we just name them set_up_X and run them sequentially.
    # Our heirs needs to define them carefully to not to step on over other,
    # but its good enough for unit tests.
    # TIP: Use set_up_X where X is between 10 and 99. X80 Basic is back :)
    def get_methods(self, starting_with, reverse=False):
        methods = []
        members = inspect.getmembers(self, predicate=inspect.ismethod)
        members = sorted(members, key=lambda m: m[0], reverse=reverse)
        for m in members:
            name, method = m
            if name.startswith(starting_with):
                methods.append(method)
        return methods

    def setUp(self):
        super(BaseTest, self).setUp()
        for method in self.get_methods("set_up"):
            method()

    def tearDown(self):
        for method in self.get_methods("tear_down", reverse=True):
            method()
        super(BaseTest, self).tearDown()

    def build_transaction_query(self, ucode=False):
        return (
            unicode('SELECT 1, 2, 3, 4;') if ucode else 'SELECT 1, 2, 3, 4;',
            unicode('SELECT 5, 6, 7, 8;') if ucode else 'SELECT 5, 6, 7, 8;',
            'SELECT 9, 10, 11, 12;',
            ('SELECT %s+10, %s+10, %s+10, %s+10;', (3, 4, 5, 6)),
            'SELECT 17, 18, 19, 20;',
            ('SELECT %s+20, %s+20, %s+20, %s+20;', (1, 2, 3, 4)),
        )

    def compare_transaction_cursors(self, cursors):
        self.assertEqual(len(cursors), 6)
        self.assertEqual(cursors[0].fetchone(), (1, 2, 3, 4))
        self.assertEqual(cursors[1].fetchone(), (5, 6, 7, 8))
        self.assertEqual(cursors[2].fetchone(), (9, 10, 11, 12))
        self.assertEqual(cursors[3].fetchone(), (13, 14, 15, 16))
        self.assertEqual(cursors[4].fetchone(), (17, 18, 19, 20))
        self.assertEqual(cursors[5].fetchone(), (21, 22, 23, 24))


class BaseDataTest(BaseTest):
    def clean_db(self):
        yield self.conn.execute("DROP TABLE IF EXISTS unit_test_large_query;")
        yield self.conn.execute("DROP TABLE IF EXISTS unit_test_transaction;")
        yield self.conn.execute("DROP TABLE IF EXISTS  unit_test_int_table;")
        yield self.conn.execute("DROP FUNCTION IF EXISTS unit_test_callproc(integer);")

    def prepare_db(self):
        yield self.conn.execute(
            "CREATE TABLE unit_test_large_query ("
            "id serial NOT NULL, name character varying, data text);"
        )
        yield self.conn.execute(
            "CREATE TABLE unit_test_transaction ("
            "id serial NOT NULL, name character varying, data text);",
        )
        yield self.conn.execute("CREATE TABLE unit_test_int_table (id integer);")
        yield self.conn.execute(
            "CREATE OR REPLACE FUNCTION unit_test_callproc(n integer)\n"
            "RETURNS integer AS $BODY$BEGIN\n"
            "RETURN n*n;\n"
            "END;$BODY$ LANGUAGE plpgsql VOLATILE;"
        )

    def fill_int_data(self, amount=1000):
        return self.conn.transaction([
            "INSERT INTO unit_test_int_table VALUES %s" % ",".join("(%s)" % i for i in range(amount)),
        ])

    @gen_test
    def set_up_10(self):
        self.conn = yield momoko.connect(self.dsn, ioloop=self.io_loop)
        for g in chain(self.clean_db(), self.prepare_db()):
            yield g

    @gen_test
    def tear_down_10(self):
        for g in self.clean_db():
            yield g


class MomokoConnectionTest(BaseTest):
    @gen_test
    def test_connect(self):
        """Test that Connection can connect to the database"""
        conn = yield momoko.connect(good_dsn, ioloop=self.io_loop)
        self.assertIsInstance(conn, momoko.Connection)

    @gen_test
    def test_bad_connect(self):
        """Test that Connection raises connection errors"""
        try:
            conn = yield momoko.connect(bad_dsn, ioloop=self.io_loop)
        except Exception as error:
            self.assertIsInstance(error, psycopg2.OperationalError)

    @gen_test
    def test_bad_connect_local(self):
        """Test that Connection raises connection errors when using local socket"""
        try:
            conn = yield momoko.connect(local_bad_dsn, ioloop=self.io_loop)
        except Exception as error:
            self.assertIsInstance(error, psycopg2.OperationalError)


class MomokoConnectionDataTest(BaseDataTest):
    @gen_test
    def test_execute(self):
        """Testing simple SELECT"""
        cursor = yield self.conn.execute("SELECT 1, 2, 3")
        self.assertEqual(cursor.fetchall(), [(1, 2, 3)])

    @gen_test
    def test_ping(self):
        """Testing ping"""
        yield self.conn.ping()

    @gen_test
    def test_large_query(self):
        """Testing support for large queries"""
        query_size = 100000
        chars = string.ascii_letters + string.digits + string.punctuation

        for n in range(5):
            random_data = ''.join([random.choice(chars) for i in range(query_size)])
            cursor = yield self.conn.execute("INSERT INTO unit_test_large_query (data) VALUES (%s) "
                                             "RETURNING data;", (random_data,))
            self.assertEqual(cursor.fetchone(), (random_data,))

        cursor = yield self.conn.execute("SELECT COUNT(*) FROM unit_test_large_query;")
        self.assertEqual(cursor.fetchone(), (5,))

    @gen_test
    def test_transaction(self):
        """Testing transaction on standalone connection"""
        cursors = yield self.conn.transaction(self.build_transaction_query())
        self.compare_transaction_cursors(cursors)

    @gen_test
    def test_unicode_transaction(self):
        """Testing transaction on standalone connection, as unicode string"""
        cursors = yield self.conn.transaction(self.build_transaction_query(True))
        self.compare_transaction_cursors(cursors)

    @gen_test
    def test_transaction_rollback(self):
        """Testing transaction auto-rollback functionality"""
        chars = string.ascii_letters + string.digits + string.punctuation
        data = ''.join([random.choice(chars) for i in range(100)])

        try:
            yield self.conn.transaction((
                ("INSERT INTO unit_test_transaction (data) VALUES (%s);", (data,)),
                "SELECT DOES NOT WORK!;"
            ), auto_rollback=True)
        except psycopg2.ProgrammingError:
            pass

        cursor = yield self.conn.execute("SELECT COUNT(*) FROM unit_test_transaction;")
        self.assertEqual(cursor.fetchone(), (0,))

    @unittest.skipIf(not test_hstore, "Skipping test as requested")
    @gen_test
    def test_hstore(self):
        """Testing hstore"""

        yield self.conn.register_hstore()

        cursor = yield self.conn.execute("SELECT 'a=>b, c=>d'::hstore;")
        self.assertEqual(cursor.fetchall(), [({"a": "b", "c": "d"},)])

        cursor = yield self.conn.execute("SELECT %s;", ({'e': 'f', 'g': 'h'},))
        self.assertEqual(cursor.fetchall(), [({"e": "f", "g": "h"},)])

    @gen_test
    def test_json(self):
        """Testing json"""
        if self.conn.server_version < 90200:
            self.skipTest("skiping test - server too old. At least 9.3 is required")

        yield self.conn.register_json()

        cursor = yield self.conn.execute('SELECT \'{"a": "b", "c": "d"}\'::json;')
        self.assertEqual(cursor.fetchall(), [({"a": "b", "c": "d"},)])

    @gen_test
    def test_callproc(self):
        """Testing callproc"""
        cursor = yield self.conn.callproc("unit_test_callproc", (64,))
        self.assertEqual(cursor.fetchone(), (4096,))

    @gen_test
    def test_query_error(self):
        """Testing that execute method propages exception properly"""
        try:
            yield self.conn.execute('SELECT DOES NOT WORK!;')
        except psycopg2.ProgrammingError:
            pass

    @gen_test
    def test_mogrify(self):
        """Testing mogrify"""
        sql = self.conn.mogrify("SELECT %s, %s;", ('\'"test"\'', "SELECT 1;"))
        if self.conn.server_version < 90100:
            self.assertEqual(sql, b"SELECT E'''\"test\"''', E'SELECT 1;';")
        else:
            self.assertEqual(sql, b"SELECT '''\"test\"''', 'SELECT 1;';")

        yield self.conn.execute(sql)

    def test_mogrify_error(self):
        """Testing that mogrify propagates exception properly"""
        try:
            self.conn.mogrify("SELECT %(foos;", {"foo": "bar"})
        except psycopg2.ProgrammingError:
            pass


class MomokoConnectionServerSideCursorTest(BaseDataTest):
    @gen_test
    def test_server_side_cursor(self):
        """Testing server side cursors support"""
        int_count = 1000
        offset = 0
        chunk = 10
        yield self.fill_int_data(int_count)

        yield self.conn.execute("BEGIN")
        yield self.conn.execute("DECLARE all_ints CURSOR FOR SELECT * FROM unit_test_int_table")
        while offset < int_count:
            cursor = yield self.conn.execute("FETCH %s FROM all_ints", (chunk,))
            self.assertEqual(cursor.fetchall(), [(i, ) for i in range(offset, offset+chunk)])
            offset += chunk
        yield self.conn.execute("CLOSE all_ints")
        yield self.conn.execute("COMMIT")


class MomokoConnectionSetsessionTest(BaseTest):
    @gen_test
    def test_setsession(self):
        """Testing that setssion parameter is honoured"""
        setsession = deque([None, "SELECT 1", "SELECT 2"])
        time_zones = ["UTC", "Israel", "Australia/Melbourne"]

        for i in range(len(time_zones)):
            setsession[i] = "SET TIME ZONE '%s'" % time_zones[i]
            conn = yield momoko.connect(self.dsn, ioloop=self.io_loop, setsession=setsession)
            cursor = yield conn.execute("SELECT current_setting('TIMEZONE');")
            self.assertEqual(cursor.fetchall(), [(time_zones[i],)])
            conn.close()
            setsession.rotate(1)


class MomokoConnectionFactoriesTest(BaseTest):
    @gen_test
    def test_cursor_factory(self):
        """Testing that cursor_factory parameter is properly propagated"""
        conn = yield momoko.connect(self.dsn, ioloop=self.io_loop, cursor_factory=RealDictCursor)
        cursor = yield conn.execute("SELECT 1 AS a")
        self.assertEqual(cursor.fetchone(), {"a": 1})

    @gen_test
    def test_connection_factory(self):
        """Testing that connection_factory parameter is properly propagated"""
        conn = yield momoko.connect(self.dsn, ioloop=self.io_loop, connection_factory=RealDictConnection)
        cursor = yield conn.execute("SELECT 1 AS a")
        self.assertEqual(cursor.fetchone(), {"a": 1})

    @gen_test
    def test_ping_with_named_cursor(self):
        """Test whether Connection.ping works fine with named cursors. Issue #74"""
        conn = yield momoko.connect(self.dsn, ioloop=self.io_loop, cursor_factory=RealDictCursor)
        yield conn.ping()


#
# Pool tests
#


class PoolBaseTest(BaseTest):
    pool_size = 3
    max_size = None
    raise_connect_errors = False

    def build_pool(self, dsn=None, setsession=(), con_factory=None, cur_factory=None, size=None, auto_shrink=False,
                   shrink_delay=datetime.timedelta(seconds=1), shrink_period=datetime.timedelta(milliseconds=500)):
        db = momoko.Pool(
            dsn=(dsn or self.dsn),
            size=(size or self.pool_size),
            max_size=self.max_size,
            ioloop=self.io_loop,
            setsession=setsession,
            raise_connect_errors=self.raise_connect_errors,
            connection_factory=con_factory,
            cursor_factory=cur_factory,
            auto_shrink=auto_shrink,
            shrink_period=shrink_period,
            shrink_delay=shrink_delay
        )
        return db.connect()

    def build_pool_sync(self, *args, **kwargs):
        f = self.build_pool(*args, **kwargs)

        # could use gen_test(lambda x: (yield f))(self)
        # but it does not work in Python 2.6 for some reason
        def runner(x):
            yield f
        gen_test(timeout=30)(runner)(self)
        return f.result()

    def close_connections(self, db, amount=None):
        amount = amount or (len(db.conns.free) + len(db.conns.busy))
        for conn in db.conns.busy.union(db.conns.free):
            if not amount:
                break
            if not conn.closed:
                conn.close()
                amount -= 1

    shutter = close_connections

    def total_close(self, db):
        self.shutter(db)
        for conn in db.conns.busy.union(db.conns.free).union(db.conns.dead):
            conn.dsn = bad_dsn

    @gen_test
    def run_and_check_query(self, db):
        cursor = yield db.execute("SELECT 6, 19, 24;")
        self.assertEqual(cursor.fetchall(), [(6, 19, 24)])

    @gen_test
    def set_up_20(self):
        self.db = yield self.build_pool()

    @gen_test
    def tear_down_00(self):  # closing pool is the last thing that should run
        self.db.close()


class PoolBaseDataTest(PoolBaseTest, BaseDataTest):
    pass


@unittest.skipIf(psycopg2_impl == "psycopg2cffi", "Skipped. See: https://github.com/chtd/psycopg2cffi/issues/49")
class ProxyMixIn(object):
    dsn = good_proxy_dsn
    good_dsn = good_proxy_dsn

    def start_proxy(self):
        # Dirty way to make sure there are no proxies leftovers
        subprocess.call(("killall", TCPPROXY_PATH), stderr=open("/dev/null", "w"))

        proxy_conf = "127.0.0.1:%s -> %s:%s" % (db_proxy_port, db_host, db_port)
        self.proxy = subprocess.Popen((TCPPROXY_PATH, proxy_conf,))
        time.sleep(0.1)

    def terminate_proxy(self):
        self.proxy.terminate()

    def kill_connections(self, db, amount=None):
        self.terminate_proxy()
        self.start_proxy()

    def set_up_00(self):
        self.start_proxy()

    def tear_down_00(self):
        self.terminate_proxy()

    shutter = kill_connections


class MomokoPoolTest(PoolBaseTest):
    @gen_test
    def test_connect(self):
        db = yield self.build_pool()
        self.assertIsInstance(db, momoko.Pool)


class MomokoPoolSetsessionTest(PoolBaseTest):
    pool_size = 1

    @gen_test
    def test_setsession(self):
        """Testing that setssion parameter is honoured"""
        setsession = deque([None, "SELECT 1", "SELECT 2"])
        time_zones = ["UTC", "Israel", "Australia/Melbourne"]

        for i in range(len(time_zones)):
            setsession[i] = "SET TIME ZONE '%s'" % time_zones[i]
            db = yield self.build_pool(setsession=setsession)
            cursor = yield db.execute("SELECT current_setting('TIMEZONE');")
            self.assertEqual(cursor.fetchall(), [(time_zones[i],)])
            db.close()
            setsession.rotate(1)


class MomokoPoolDataTest(PoolBaseDataTest, MomokoConnectionDataTest):
    @gen_test
    def set_up_30(self):
        self.conn = self.db

    def tear_down_30(self):
        self.assertEqual(len(self.db.conns.busy), 0, msg="Some connections were not recycled")

    # Pool's mogirify is async -> copy/paste
    @gen_test
    def test_mogrify(self):
        """Testing mogrify"""
        sql = yield self.conn.mogrify("SELECT %s, %s;", ('\'"test"\'', "SELECT 1;"))
        if self.conn.server_version < 90100:
            self.assertEqual(sql, b"SELECT E'''\"test\"''', E'SELECT 1;';")
        else:
            self.assertEqual(sql, b"SELECT '''\"test\"''', 'SELECT 1;';")

        yield self.conn.execute(sql)

    # Pool's mogirify is async -> copy/paste
    @gen_test
    def test_mogrify_error(self):
        """Testing that mogrify propagates exception properly"""
        try:
            yield self.conn.mogrify("SELECT %(foos;", {"foo": "bar"})
        except psycopg2.ProgrammingError:
            pass

    @gen_test
    def test_transaction_with_reconnect(self):
        """Test whether transaction works after connections were closed"""

        # Added result counting, since there was a bug in retry mechanism that caused
        # double-execution of query after reconnect
        self.shutter(self.db)
        yield self.db.transaction(("INSERT INTO unit_test_int_table VALUES (1)",))
        cursor = yield self.db.execute("SELECT COUNT(1) FROM unit_test_int_table")
        self.assertEqual(cursor.fetchall(), [(1,)])
        log.debug("test done")

    @gen_test
    def test_getconn_putconn(self):
        """Testing getconn/putconn functionality"""
        for i in range(self.pool_size * 5):
            # Run many times to check that connections get recycled properly
            conn = yield self.db.getconn()
            for j in range(10):
                cursor = yield conn.execute("SELECT %s", (j,))
                self.assertEqual(cursor.fetchall(), [(j, )])
            self.db.putconn(conn)

    @gen_test
    def test_getconn_putconn_with_reconnect(self):
        """Testing getconn/putconn functionality with reconnect after closing connections"""
        for i in range(self.pool_size * 5):
            # Run many times to check that connections get recycled properly
            self.shutter(self.db)
            conn = yield self.db.getconn()
            for j in range(10):
                cursor = yield conn.execute("SELECT %s", (j,))
                self.assertEqual(cursor.fetchall(), [(j, )])
            self.db.putconn(conn)

    @gen_test
    def test_getconn_manage(self):
        """Testing getcontest_getconn_putconn + context manager functionality"""
        for i in range(self.pool_size * 5):
            # Run many times to check that connections get recycled properly
            conn = yield self.db.getconn()
            with self.db.manage(conn):
                for j in range(10):
                    cursor = yield conn.execute("SELECT %s", (j,))
                    self.assertEqual(cursor.fetchall(), [(j, )])

    @gen_test
    def test_getconn_manage_with_reconnect(self):
        """Testing getcontest_getconn_putconn_with_reconnect + context manager functionality"""
        for i in range(self.pool_size * 5):
            # Run many times to check that connections get recycled properly
            self.shutter(self.db)
            conn = yield self.db.getconn()
            with self.db.manage(conn):
                for j in range(10):
                    cursor = yield conn.execute("SELECT %s", (j,))
                    self.assertEqual(cursor.fetchall(), [(j, )])

    @gen_test
    def test_getconn_manage_with_exception(self):
        """Testing getconn + context manager functionality + deliberate exception"""
        self.shutter(self.db)
        conn = yield self.db.getconn(ping=False)
        with self.db.manage(conn):
            try:
                cursor = yield conn.execute("SELECT 1")
            except psycopg2.Error as error:
                pass
        self.assertEqual(len(self.db.conns.busy), 0, msg="Some connections were not recycled")


class MomokoPoolDataTestProxy(ProxyMixIn, MomokoPoolDataTest):
    pass


class MomokoPoolServerSideCursorTest(PoolBaseDataTest):
    @gen_test
    def test_server_side_cursor(self):
        """Testing server side cursors support"""
        int_count = 1000
        offset = 0
        chunk = 10
        yield self.fill_int_data(int_count)

        conn = yield self.db.getconn()
        with self.db.manage(conn):
            yield conn.execute("BEGIN")
            yield conn.execute("DECLARE all_ints CURSOR FOR SELECT * FROM unit_test_int_table")
            while offset < int_count:
                cursor = yield conn.execute("FETCH %s FROM all_ints", (chunk,))
                self.assertEqual(cursor.fetchall(), [(i, ) for i in range(offset, offset+chunk)])
                offset += chunk
            yield conn.execute("CLOSE all_ints")
            yield conn.execute("COMMIT")


class MomokoPoolFactoriesTest(PoolBaseTest):
    @gen_test
    def test_cursor_factory(self):
        """Testing that cursor_factory parameter is properly propagated"""
        db = yield self.build_pool(cur_factory=RealDictCursor)
        cursor = yield db.execute("SELECT 1 AS a")
        self.assertEqual(cursor.fetchone(), {"a": 1})

    @gen_test
    def test_connection_factory(self):
        """Testing that connection_factory parameter is properly propagated"""
        db = yield self.build_pool(con_factory=RealDictConnection)
        cursor = yield db.execute("SELECT 1 AS a")
        self.assertEqual(cursor.fetchone(), {"a": 1})

    @gen_test
    def test_cursor_factory_with_extensions(self):
        """Testing that NamedTupleCursor factory is working with hstore and json"""
        db = yield self.build_pool(cur_factory=NamedTupleCursor)

        yield db.register_hstore()
        yield db.register_json()

        cursor = yield self.db.execute("SELECT 'a=>b, c=>d'::hstore;")
        self.assertEqual(cursor.fetchall(), [({"a": "b", "c": "d"},)])

        cursor = yield self.db.execute("SELECT %s;", ({'e': 'f', 'g': 'h'},))
        self.assertEqual(cursor.fetchall(), [({"e": "f", "g": "h"},)])


class MomokoPoolParallelTest(PoolBaseTest):
    pool_size = 1

    def run_parallel_queries(self, jobs=None):
        """Testing that pool queries database in parallel"""
        jobs = jobs or max(self.pool_size, self.max_size if self.max_size else 0)
        query_sleep = 2
        sleep_time = query_sleep * float(jobs/self.pool_size)

        def func(self):
            to_yield = []
            for i in range(jobs):
                to_yield.append(self.db.execute('SELECT pg_sleep(%s);' % query_sleep))
            yield to_yield

        start_time = time.time()
        gen_test(func)(self)
        execution_time = time.time() - start_time
        self.assertLess(execution_time, sleep_time*1.10, msg="Query execution was too long")

    def test_parallel_queries(self):
        """Testing that pool queries database in parallel"""
        self.run_parallel_queries()
        # and once again to test that connections can be reused properly
        self.run_parallel_queries()

    def test_request_queueing(self):
        """Test that pool queues outstaning requests when all connections are busy"""
        self.run_parallel_queries(self.pool_size*2)

    def test_parallel_queries_after_reconnect_all(self):
        """Testing that pool still queries database in parallel after ALL connections were closeded"""
        self.shutter(self.db)
        self.run_parallel_queries()

    def test_parallel_queries_after_reconnect_some(self):
        """Testing that pool still queries database in parallel after SOME connections were closed"""
        self.shutter(self.db, amount=self.pool_size/2)
        self.run_parallel_queries()


class MomokoPoolParallelTestProxy(ProxyMixIn, MomokoPoolParallelTest):
    pass


class MomokoPoolStretchTest(MomokoPoolParallelTest):
    pool_size = 1
    max_size = 5

    def test_parallel_queries(self):
        """Run parallel queies and check that pool size matches number of jobs"""
        jobs = self.max_size - 1
        super(MomokoPoolStretchTest, self).run_parallel_queries(jobs)
        self.assertEqual(self.db.conns.total, jobs)

    def test_dont_stretch(self):
        """Testing that we do not stretch unless needed"""
        self.run_and_check_query(self.db)
        self.run_and_check_query(self.db)
        self.run_and_check_query(self.db)
        self.assertEqual(self.db.conns.total, self.pool_size+1)

    def test_dont_stretch_after_reconnect(self):
        """Testing that reconnecting dead connection does not trigger pool stretch"""
        self.shutter(self.db)
        self.test_dont_stretch()

    def test_stretch_after_disonnect(self):
        """Testing that stretch works after disconnect"""
        self.shutter(self.db)
        self.test_parallel_queries()

    @gen_test
    def test_stretch_genconn(self):
        """Testing that stretch works together with get/putconn"""
        f1 = self.db.getconn()
        f2 = self.db.getconn()
        f3 = self.db.getconn()
        yield [f1, f2, f3]

        conn1 = f1.result()
        conn2 = f2.result()
        conn3 = f3.result()

        f1 = conn1.execute("SELECT 1;")
        f2 = conn2.execute("SELECT 2;")
        f3 = conn3.execute("SELECT 3;")

        yield [f1, f2, f3]

        cursor1 = f1.result()
        cursor2 = f2.result()
        cursor3 = f3.result()

        self.assertEqual(cursor1.fetchone(), (1,))
        self.assertEqual(cursor2.fetchone(), (2,))
        self.assertEqual(cursor3.fetchone(), (3,))

        for conn in conn1, conn2, conn3:
            self.db.putconn(conn)

        self.assertEqual(self.db.conns.total, 3)


class MomokoPoolStretchTestProxy(ProxyMixIn, MomokoPoolStretchTest):
    pass


class MomokoPoolVolatileDbTest(PoolBaseTest):
    pool_size = 3

    @gen_test
    def test_startup(self):
        """Testing that all connections are dead after pool init with bad dsn"""
        db = yield self.build_pool(dsn=bad_dsn)
        self.assertEqual(self.pool_size, len(db.conns.dead))

    @gen_test
    def test_startup_local(self):
        """Testing that we catch early exeception with local connections"""
        db = yield self.build_pool(dsn=local_bad_dsn)
        self.assertEqual(self.pool_size, len(db.conns.dead))

    def test_reconnect(self):
        """Testing if we can reconnect if connections dies"""
        db = self.build_pool_sync(dsn=self.good_dsn)
        self.shutter(db)
        self.run_and_check_query(db)

    def test_reconnect_interval_good_path(self):
        """Testing that we can recover if database was down during startup"""
        db = self.build_pool_sync(dsn=bad_dsn)
        self.assertEqual(self.pool_size, len(db.conns.dead))
        for conn in db.conns.dead:
            conn.dsn = good_dsn
        time.sleep(db.reconnect_interval)
        self.run_and_check_query(db)

    def test_reconnect_interval_bad_path(self):
        """Testing that pool does not try to reconnect right after last connection attempt failed"""
        db = self.build_pool_sync(dsn=bad_dsn)
        self.assertEqual(self.pool_size, len(db.conns.dead))
        for conn in db.conns.dead:
            conn.dsn = good_dsn
        try:
            self.run_and_check_query(db)
        except psycopg2.DatabaseError:
            pass

    @gen_test
    def test_ping_error(self):
        """Test that getconn uses ping properly to detect database unavailablity"""
        db = yield self.build_pool(dsn=self.good_dsn, size=3)
        self.total_close(db)
        try:
            yield db.getconn()
        except db.DatabaseNotAvailable as err:
            pass

    @gen_test
    def test_abort_waiting_queue(self):
        """Testing that waiting queue is aborted properly when all connections are dead"""
        db = yield self.build_pool(dsn=self.good_dsn, size=1)
        f1 = db.execute("SELECT 1")
        f2 = db.execute("SELECT 1")

        self.assertEqual(len(db.conns.waiting_queue), 1)

        f1.add_done_callback(lambda f: self.total_close(db))

        try:
            yield [f1, f2]
        except psycopg2.DatabaseError:
            pass
        self.assertEqual(len(db.conns.waiting_queue), 0)


class MomokoPoolVolatileDbTestProxy(ProxyMixIn, MomokoPoolVolatileDbTest):
    pass


class MomokoPoolPartiallyConnectedTest(PoolBaseTest):
    raise_connect_errors = True
    pool_size = 3

    def test_partially_connected(self):
        """Test that PartiallyConnected is raised properly"""
        exp = momoko.exceptions.PartiallyConnectedError
        self.assertRaises(exp, self.build_pool_sync, dsn=bad_dsn)


class MomokoPoolShrinkTest(MomokoPoolParallelTest):
    pool_size = 2
    max_size = 5

    @gen_test
    def test_pool_shrinking(self):
        db = yield self.build_pool(auto_shrink=True, shrink_delay=datetime.timedelta(seconds=1),
                                   shrink_period=datetime.timedelta(milliseconds=500))
        f1 = db.execute("SELECT 1")
        f2 = db.execute("SELECT 2")
        f3 = db.execute("SELECT 3")
        f4 = db.execute("SELECT 4")
        f5 = db.execute("SELECT 5")
        cursors = yield [f1, f2, f3, f4, f5]
        yield gen.sleep(.7)

        self.assertEqual(db.conns.total, 5)
        self.assertEqual(cursors[0].fetchone()[0], 1)
        self.assertEqual(cursors[1].fetchone()[0], 2)
        self.assertEqual(cursors[2].fetchone()[0], 3)
        self.assertEqual(cursors[3].fetchone()[0], 4)
        self.assertEqual(cursors[4].fetchone()[0], 5)

        yield gen.sleep(1)

        self.assertEqual(db.conns.total, 2)

    @gen_test
    def test_pool_shrinking_with_shrink_delay(self):
        db = yield self.build_pool(auto_shrink=True, shrink_delay=datetime.timedelta(seconds=1),
                                   shrink_period=datetime.timedelta(milliseconds=500))
        f1 = db.execute("SELECT 1")
        f2 = db.execute("SELECT 2")
        f3 = db.execute("SELECT 3")
        f4 = db.execute("SELECT 4")
        f5 = db.execute("SELECT 5")
        cursors = yield [f1, f2, f3, f4, f5]
        yield gen.sleep(.7)

        self.assertEqual(db.conns.total, 5)
        self.assertEqual(cursors[0].fetchone()[0], 1)
        self.assertEqual(cursors[1].fetchone()[0], 2)
        self.assertEqual(cursors[2].fetchone()[0], 3)
        self.assertEqual(cursors[3].fetchone()[0], 4)
        self.assertEqual(cursors[4].fetchone()[0], 5)

        f1 = db.execute("SELECT 1")
        f2 = db.execute("SELECT 2")
        f3 = db.execute("SELECT 3")
        cursors = yield [f1, f2, f3]
        self.assertEqual(cursors[0].fetchone()[0], 1)
        self.assertEqual(cursors[1].fetchone()[0], 2)
        self.assertEqual(cursors[2].fetchone()[0], 3)

        yield gen.sleep(1)

        self.assertEqual(db.conns.total, 3)

if __name__ == '__main__':
    if debug:
        FORMAT = '%(asctime)-15s %(levelname)s:%(name)s %(funcName)-15s: %(message)s'
        logging.basicConfig(format=FORMAT)
        logging.getLogger("momoko").setLevel(logging.DEBUG)
        logging.getLogger("unittest").setLevel(logging.DEBUG)
    unittest.main()
