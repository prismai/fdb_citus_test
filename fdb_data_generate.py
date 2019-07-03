from   datetime import datetime
from multiprocessing import Pool
import fdb
from   fdb.tuple import pack, unpack
import random
import string
import uuid
import logging
import argparse

fdb.api_version(610)

def generate_data(N):
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=N))

@fdb.transactional
def add(tr, bsize):
    for i in range(bsize):
        data = generate_data(1024).encode('utf-8')
        now = str(datetime.utcnow().timestamp())
        uid = str(uuid.uuid4().hex)
        key = pack((now, uid))
        tr.set(key, data)

def add_batch_process(lim, bsize):
    db = fdb.open()
    i = 0
    while i < lim if lim else 1:
        try:
            add(db, bsize)
        except Exception:
            logging.exception('')
        if lim:
            i = i + 1

parser = argparse.ArgumentParser(description='FDB data generation app')
parser.add_argument('num_threads', type=int, help='Number of threads to run data generation')
parser.add_argument('batch_size', type=int, help='Commit batch size')
parser.add_argument('--num_records', type=int, default=None, help='Records per thread, if not set will run indefinitely')
args = parser.parse_args()
num_proc = args.num_threads
num_records = args.num_records
batch_size = args.batch_size

p = Pool(num_proc)
multiple_results = [p.apply_async(add_batch_process, (num_records, batch_size,)) for i in range(num_proc)]
[res.get(timeout=None) for res in multiple_results]
