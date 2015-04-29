from __future__ import print_function
from __future__ import absolute_import

import threading

from tests import *

"""
Quick and dirty performance test - async vs threads.
By default Postgresql is configured to support up 100 connections.
Change max_connections to 1100 in /etc/postgresql/9.4/main/postgresql.conf
to run this example.

NOTE: This benchmark is purely synthetic. In real life you'll be bound by database
query throughput!

So far, on psycopg2, momoko pool "context switching" is about 4 times slower compared to
python thread contenxt switching when issueing queries that do nothing (SELECT 1).
On my laptop momoko.Pool can do only about 8000 qps, but simple consequtive threading
can do over 30,000.

On psycopg2cffi, things are bit better - momoko.Pool only about 50% slower.
But that's only because psycopg2cffi is less performant.

After chaning queries to actually do something (SELECT pg_sleep(0.002)) I was able to
actually measure thread (or Pool) overhead more accurately.

This is the typical run:

Threads(1): 27.04 seconds
Pool(1): 28.72 seconds
Threads(10): 2.34 seconds
Pool(10): 2.67 seconds
Threads(100): 0.62 seconds
Pool(100): 1.38 seconds
Threads(1000): 1.16 seconds
Pool(1000): 1.50 seconds

Looks like threads are at their best when their number is about 100.
At concurrency of 1000, threads get 87% penalty while pool takes only 8%.
"""


class MomokoPoolPerformanceTest(PoolBaseTest):
    pool_size = 1
    amount = 10000
    query = "SELECT pg_sleep(0.002)"

    def run_thread_queries(self, amount=amount, thread_num=1):
        conns = []
        for i in range(thread_num):
            conn = psycopg2.connect(good_dsn)
            conns.append(conn)

        def runner(conn):
            for i in range(int(amount/thread_num)):
                with conn.cursor() as cur:
                    cur.execute(self.query)
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
            for j in range(amount):
                futures.append(db.execute(self.query))
            yield futures

        gen_test(timeout=300)(runner)(self)
        delta = time.time() - start
        db.close()
        return delta

    def test_perf(self):
        print("\n")
        for threads in (1, 10, 100, 1000):
            print("Threads(%s): %.2f seconds" % (threads, self.run_thread_queries(thread_num=threads)))
            print("Pool(%s): %.2f seconds" % (threads, self.run_pool_queries(thread_num=threads)))


if __name__ == '__main__':
    if debug:
        FORMAT = '%(asctime)-15s %(levelname)s:%(name)s %(funcName)-15s: %(message)s'
        logging.basicConfig(format=FORMAT)
        logging.getLogger("momoko").setLevel(logging.DEBUG)
        logging.getLogger("unittest").setLevel(logging.DEBUG)
    unittest.main()
