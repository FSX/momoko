from __future__ import print_function
from __future__ import absolute_import

import threading

from tests import *

"""
Quick and dirty performance test - async vs threads.
By default Postgresql is configured to support up 100 connections.
Change max_connections to 1100 in /etc/postgresql/9.4/main/postgresql.conf
to run this example.

So far, on psycopg2, momoko pool "context switching" is about 4 times slower compared to
python thread contenxt switching when issueing queries that do nothing (SELECT 1).
On my laptop momoko.Pool can do only about 8000 qps, but simple consequtive threading
can do over 30,000.

On psycopg2cffi, things are bit better - momoko.Pool only about 50% slower.
But that's only because psycopg2cffi is less performant.

On top of that momoko uses 10 times more memory at about 1Gb on python2.7.

NOTE: This benchmark is purely synthetic. In real life you'll be bound by database
query throughput.

"""


class MomokoPoolPerformanceTest(PoolBaseTest):
    pool_size = 1
    amount = 100000

    def run_thread_queries(self, amount=amount, thread_num=1):
        conns = []
        for i in range(thread_num):
            conn = psycopg2.connect(good_dsn)
            conns.append(conn)

        def runner(conn):
            for i in range(amount/thread_num):
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    cur.fetchall()

        start = time.time()
        threads = []
        for conn in conns:
            thread = threading.Thread(target=runner, args=(conn,))
            thread.start()
            threads.append(thread)
        for thread in threads:
            thread.join()
        delta = time.time() - start
        for conn in conns:
            conn.close()
        return delta

    def run_pool_queries(self, amount=amount, thread_num=1):

        db = self.build_pool_sync(dsn=good_dsn, size=thread_num)
        start = time.time()

        def runner(x):
            futures = []
            for i in range(amount):
                futures.append(db.execute("SELECT 1"))
            yield futures

        gen_test(timeout=30)(runner)(self)
        delta = time.time() - start
        db.close()
        return delta

    def test_single(self):
        print("\n")
        print("Threads: %.2f seconds" % self.run_thread_queries())
        print("Pool: %.2f seconds" % self.run_pool_queries())

    def test_multiple(self):
        print("\n")
        for threads in (10, 100, 1000):
            print("Threads(%s): %.2f seconds" % (threads, self.run_thread_queries(thread_num=threads)))
            print("Pool(%s): %.2f seconds" % (threads, self.run_pool_queries(thread_num=threads)))


if __name__ == '__main__':
    if debug:
        FORMAT = '%(asctime)-15s %(levelname)s:%(name)s %(funcName)-15s: %(message)s'
        logging.basicConfig(format=FORMAT)
        logging.getLogger("momoko").setLevel(logging.DEBUG)
        logging.getLogger("unittest").setLevel(logging.DEBUG)
    unittest.main()
