from  datetime import datetime
import logging
import argparse
import time
import psycopg2


parser = argparse.ArgumentParser(description='CITUS data deletion app')
parser.add_argument('port', type=int, help='PostgreSQL server port')
parser.add_argument('dbname', type=int, help='DB name')
parser.add_argument('timeout', type=int, help='Data expiration rule in minutes')
args = parser.parse_args()
timeout = args.timeout
conn_string = "port='{port}' dbname='{dbname}'".format(
    port=args.port,
    dbname=args.dbname,
)

conn = psycopg2.connect(conn_string)
try:
    while True:
        SQL = "SELECT partman.run_maintenance('public.a00', p_analyze := false);"
        with conn.cursor() as curs:
            curs.execute(SQL)
            conn.commit()
        time.sleep(timeout*60)
finally:
    conn.close()
