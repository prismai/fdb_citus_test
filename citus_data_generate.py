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

def add(conn):
    data = generate_data(1024).encode('utf-8')
    now = str(datetime.utcnow().timestamp())
    uid = str(uuid.uuid4().hex)
    SQL = "INSERT {}"
    with conn.cursor() as curs:
        curs.execute(SQL)

def add_batch_process(conn_string, lim):
	conn = psycopg2.connect(conn_string)
    i = 0
    while i < lim if lim else 1:
        try:
            add(conn)
        except Exception:
            logging.exception('')
        if lim:
            i = i + 1

parser = argparse.ArgumentParser(description='FDB data generation app')
parser.add_argument('host', type=int, help='PostgreSQL server hostname')
parser.add_argument('port', type=int, help='PostgreSQL server port')
parser.add_argument('dbname', type=int, help='DB name')
parser.add_argument('user', type=int, help='DB user')
parser.add_argument('pwd', type=int, help='DB password')
parser.add_argument('num_threads', type=int, help='Number of threads to run data generation')
parser.add_argument('--num_records', type=int, default=None, help='Records per thread, if not set will run indefinitely')
args = parser.parse_args()
num_proc = args.num_threads
num_records = args.num_records
conn_string = "host='{host}' port='{port}' dbname='{dbname}' user='{user}' password={pwd}".format(
    host = args.host,
    port = args.port,
    dbname = args.dbname,
    user = args.user,
    pwd = args.pwd
)

p = Pool(num_proc)
multiple_results = [p.apply_async(add_batch_process, (conn_string, num_records,)) for i in range(num_proc)]
[res.get(timeout=None) for res in multiple_results]
