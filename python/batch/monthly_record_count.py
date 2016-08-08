# -*- coding: utf-8 -*-
import os
import os.path
import sys
import time
import json
import datetime
from contextlib import closing


import psycopg2

import click


def next_month(y, m):
    if m == 12:
        return (y + 1, 1)
    else:
        return (y, m + 1)


def open_connection(config):
    pg_host = config['host']
    pg_db = config['db']
    pg_user = config['user']
    pg_pw = config['pw']

    connect_info = 'host=%s dbname=%s user=%s password=%s' % (
        pg_host, pg_db, pg_user, pg_pw)
    return psycopg2.connect(connect_info)


def fetch_observation_id_list(conn):
    ret = set([])
    cur = conn.cursor()
    with closing(cur):
        sql = 'SELECT id FROM observation;'
        cur.execute(sql)
        for ritem in cur:
            obid = ritem[0]
            ret.add(obid)
    return ret


def determine_start_ym(conn):
    sql = 'select year, month from monthly_record_count order by year desc, month desc limit 1;'
    cur = conn.cursor()
    with closing(cur):
        cur.execute(sql)
        row = cur.fetchone()
        if row is None:
            sql2 = 'select day from daily_record_count order by day limit 1;'
            cur2 = conn.cursor()
            with closing(cur2):
                cur2.execute(sql2)
                result = cur2.fetchone()
                if result is None:
                    return None
                oldest_recorded_day = result[0]
                return (oldest_recorded_day.year, oldest_recorded_day.month)

        last_y = row[0]
        last_m = row[1]
        return next_month(last_y, last_m)


def log(msg):
    print '%s %s' % (datetime.datetime.now(), msg)


@click.command()
@click.option('-c', '--config-file')
def main(config_file):
    if not config_file or not os.path.exists(config_file):
        print 'missing config file: %s' % config_file
        sys.exit(-1)

    log('@@@@@ start')

    with open(config_file) as fh:
        config = json.load(fh)

    now = datetime.datetime.now()
    now_y = now.year
    now_m = now.month
    dt_now_ym = datetime.date(now_y, now_m, 1)

    conn = open_connection(config)
    with closing(conn):
        start_ym = determine_start_ym(conn)
        if start_ym is None:
            return

        all_obids = fetch_observation_id_list(conn)

        ym = start_ym
        dt_ym = datetime.date(ym[0], ym[1], 1)
        while dt_ym < dt_now_ym:
            t1 = time.time()
            log('%04d-%02d start' % (ym[0], ym[1]))
            cur = conn.cursor()
            with closing(cur):
                sql = 'select observation_id, extract(year from day) as year, extract(month from day) as month, sum(daily_total_count) from daily_record_count where %s <= day and day < %s group by observation_id, year, month;'

                tmp_f = '/tmp/sr2-monthly-%04d%02d.tsv' % (ym[0], ym[1])
                log('    %04d-%02d tmp file build start: %s (%.3fsec passed)' % (ym[0], ym[1], tmp_f, time.time() - t1))
                saved = 0
                wrote_obids = set([])
                with open(tmp_f, 'wb') as fh:
                    next_ym = next_month(ym[0], ym[1])
                    dt_next_ym = datetime.date(next_ym[0], next_ym[1], 1)
                    cur.execute(sql, (dt_ym, dt_next_ym))
                    for obid, y, m, ym_total in cur:
                        line = '%d\t%d\t%d\t%d\n' % (obid, y, m, ym_total)
                        fh.write(line)
                        wrote_obids.add(obid)
                        saved += 1
                        if saved % 1000 == 0:
                            log('    %04d-%02d wrote %d lines (%.3fsec passed)' % (ym[0], ym[1], saved, time.time() - t1))

                    for obid in all_obids:
                        if obid not in wrote_obids:
                            line = '%d\t%d\t%d\t%d\n' % (obid, ym[0], ym[1], 0)
                            fh.write(line)
                            wrote_obids.add(obid)
                            saved += 1
                            if saved % 1000 == 0:
                                log('    %04d-%02d wrote %d lines (%.3fsec passed)' % (ym[0], ym[1], saved, time.time() - t1))
                    log('    %04d-%02d tmp file build finished (%.3fsec passed)' % (ym[0], ym[1], time.time() - t1))

            with open(tmp_f, 'rb') as fh:
                cur = conn.cursor()
                with closing(cur):
                    log('    %04d-%02d COPY start (%.3fsec passed)' % (ym[0], ym[1], time.time() - t1))
                    cur.copy_from(
                        fh,
                        'monthly_record_count',
                        sep='\t',
                        size=1024 * 64,
                        columns=['observation_id', 'year', 'month', 'monthly_total_count']
                    )
                    log('    %04d-%02d COPY finished (%.3fsec passed)' % (ym[0], ym[1], time.time() - t1))
                conn.commit()
            os.remove(tmp_f)
            log('    %04d-%02d tmp file removed (%.3fsec passed)' % (ym[0], ym[1], time.time() - t1))
            t2 = time.time()
            log('    %04d-%02d finished, t=%.3fsec' % (ym[0], ym[1], t2 - t1))
            ym = next_month(ym[0], ym[1])
            dt_ym = datetime.date(ym[0], ym[1], 1)

    log('@@@@@ end')


if __name__ == '__main__':
    main()
