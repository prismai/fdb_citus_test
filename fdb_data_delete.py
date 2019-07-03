from  datetime import datetime, timedelta
from multiprocessing import Pool
import fdb
from   fdb.tuple import pack, unpack
import random
import string
import uuid
import logging
import argparse
import time

fdb.api_version(610)

@fdb.transactional
def delete_old_records(tr, timeout, bsize):
    now = str((datetime.utcnow() - timedelta(minutes=timeout)).timestamp())
    tr.clear_range(pack(('/x00', '')), pack((now,'')))


parser = argparse.ArgumentParser(description='FDB data expiration app')
parser.add_argument('timeout', type=int, help='Data expiration rule in minutes')
args = parser.parse_args()
timeout = args.timeout
bsize = 0

db = fdb.open()

while True:
    delete_old_records(db, timeout, bsize)
    time.sleep(1)