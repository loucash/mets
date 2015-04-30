#!/usr/bin/env python

import sqlite3
import datetime
import random

DEVICES = 100
DBFILE = "most%d.db" % (DEVICES, )
METRICS = 15
INTERVAL = 2     # seconds


def open_conn():
    return sqlite3.connect(DBFILE)


def create_tables(conn):
    c = conn.cursor()
    c.execute('''
              CREATE TABLE IF NOT EXISTS metrics (
              device_id TEXT NOT NULL,
              metric    TEST NOT NULL,
              ts        INTEGER NOT NULL,
              value     NUMERIC NOT NULL,
              PRIMARY KEY(device_id, metric, ts))
              ''')


def load_metrics(conn, device_no):
    print "load_metrics for ", device_no
    device_id = "device_%d" % (device_no, )
    now = datetime.datetime.utcnow()
    now_ts = int(now.strftime("%s"))
    start = now - datetime.timedelta(weeks=1)
    ts = int(start.strftime("%s"))
    while ts < now_ts:
        for metric_no in xrange(METRICS):
            metric_id = "metric_%d" % (metric_no, )
            value = random.randint(0, 100)
            conn.execute("INSERT INTO metrics VALUES (?,?,?,?)",
                         (device_id, metric_id, ts, value))
        ts += INTERVAL
    conn.commit()


def close_conn(conn):
    conn.close()


def main():
    conn = open_conn()
    create_tables(conn)
    for device_no in xrange(DEVICES):
        load_metrics(conn, device_no)
    close_conn(conn)

if __name__ == "__main__":
    main()
