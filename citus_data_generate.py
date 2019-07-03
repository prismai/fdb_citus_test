from  datetime import datetime
from multiprocessing import Pool
import random
import string
import uuid
import logging
import argparse
import psycopg2


def generate_data(N):
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=N))

def add(conn, bsize, dsize):
    with conn.cursor() as curs:
        for i in range(bsize):
            data = generate_data(dsize)
            uid = str(uuid.uuid4().hex)
            SQL = "INSERT INTO a00 (FEED_ID, CREATED, LABEL) VALUES (1, now(), '{}'::text);".format(data)
            curs.execute(SQL)
        conn.commit()

def add_batch_process(conn_string, lim, bsize, dsize):
    conn = psycopg2.connect(conn_string)
    i = 0
    while i < lim if lim else 1:
        try:
            add(conn, bsize, dsize)
        except Exception:
            logging.exception('')
        if lim:
            i = i + 1
    conn.close()

parser = argparse.ArgumentParser(description='FDB data generation app')
parser.add_argument('port', type=int, help='PostgreSQL server port')
parser.add_argument('dbname', type=str, help='DB name')
parser.add_argument('num_threads', type=int, help='Number of threads to run data generation')
parser.add_argument('batch_size', type=int, help='Commit batch size')
parser.add_argument('data_size', type=int, help='Data buffer size (bytes)')
parser.add_argument('--num_records', type=int, default=None, help='Records per thread, if not set will run indefinitely')
args = parser.parse_args()
num_proc = args.num_threads
num_records = args.num_records
batch_size = args.batch_size
data_size = args.data_size
conn_string = "port='{port}' dbname='{dbname}'".format(
    port=args.port,
    dbname=args.dbname,
)

p = Pool(num_proc)
multiple_results = [p.apply_async(add_batch_process, (conn_string, num_records, batch_size, data_size,)) for i in range(num_proc)]
[res.get(timeout=None) for res in multiple_results]
