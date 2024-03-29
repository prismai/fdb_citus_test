from   datetime import datetime
from multiprocessing import Pool
import fdb
from   fdb.tuple import pack, unpack
import random
import string
import uuid
import logging
import argparse
import random

fdb.api_version(610)

def generate_data(N):
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=N))

@fdb.transactional
def add(tr, bsize, dsize, rand_size):
    for i in range(bsize):
        data = generate_data(random.randint(1, dsize)).encode('utf-8')
        now = str(datetime.utcnow().timestamp())
        feed_id = random.randint(1, 20000)
        uid = str(uuid.uuid4().hex)
        key = pack((now, feed_id, uid))
        tr.set(key, data)

def add_batch_process(lim, bsize, dsize, rand_size):
    db = fdb.open()
    i = 0
    while i < lim if lim else 1:
        try:
            add(db, bsize, dsize, rand_size)
        except Exception:
            logging.exception('')
        if lim:
            i = i + 1

parser = argparse.ArgumentParser(description='FDB data generation app')
parser.add_argument('num_threads', type=int, help='Number of threads to run data generation')
parser.add_argument('batch_size', type=int, help='Commit batch size')
parser.add_argument('data_size', type=int, help='Data buffer size (bytes)')
parser.add_argument('--random_dsize', action='store_true', default=False)
parser.add_argument('--num_records', type=int, default=None, help='Records per thread, if not set will run indefinitely')
args = parser.parse_args()
num_proc = args.num_threads
num_records = args.num_records
batch_size = args.batch_size
data_size = args.data_size
rand_size = args.random_dsize

p = Pool(num_proc)
multiple_results = [p.apply_async(add_batch_process, (num_records, batch_size, data_size, rand_size)) for i in range(num_proc)]
[res.get(timeout=None) for res in multiple_results]
