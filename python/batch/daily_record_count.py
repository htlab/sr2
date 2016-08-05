# -*- coding: utf-8 -*-
import os
import os.path
import sys
import json
from contextlib import closing
import calendar
import time
import datetime

import click

import psycopg2


def next_day(y, m, d):
    month_last_day = calendar.monthrange(y, m)[1]
    if d == month_last_day:
        if m == 12:
            return (y + 1, 1, 1)
        else:
            return (y, m + 1, 1)
    else:
        return (y, m, d + 1)


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


def determine_start_day(conn):
    # determine start point
    sql = 'select day from daily_record_count order by day desc limit 1;'
    cur = conn.cursor()
    with closing(cur):
        cur.execute(sql)
        row = cur.fetchone()
        if row is None:
            last_day = None
        else:
            last_day = row[0]
            start_day = next_day(last_day.year, last_day.month, last_day.day)

    if last_day is None:
        sql = 'select created from record order by created asc limit 1;'
        cur = conn.cursor()
        with closing(cur):
            cur.execute(sql)
            row = cur.fetchone()
            if row is None:
                return None  # TODO
            first_timestamp = row[0]
            start_day = (first_timestamp.year, first_timestamp.month, first_timestamp.day)

    return start_day


# def unit_numbers(unit, numbers):
#     ret = []
#     buf = 0
#     buf_count = 0
#     for item in numbers:
#         buf += item
#         buf_count += 1
#         if buf_count == unit:
#             ret.append(buf)
#             buf = 0
#             buf_count = 0
#     assert buf_count == 0  # 割り切れる数字のはず
#     return buf
def unit_numbers(unit, numbers):
    n_unit = len(numbers) / unit
    return [ sum(numbers[unit * i:unit * (i + 1)]) for i in xrange(n_unit) ]


def save_units(conn, daily_record_count_id, unit, unit1_counts):
    unit_n_counts = unit_numbers(unit, unit1_counts)

    placeholders = ','.join(['(%s,%s,%s,%s)'] * len(unit_n_counts))
    sql = 'INSERT INTO daily_unit(daily_record_count_id, unit, unit_seq, count) VALUES ' + placeholders + ';'

    sql_args = []
    for i, count in enumerate(unit_n_counts):
        sql_args.append(daily_record_count_id)
        sql_args.append(unit)
        sql_args.append(i)
        sql_args.append(count)

    sql_args = tuple(sql_args)

    cur = conn.cursor()
    with closing(cur):
        cur.execute(sql, sql_args)
    # print '    saved: unit=%d' % unit


def save_daily_report(day, conn, obid, ob_data):
    # calculation
    # print 'save: %04d-%02d-%02d, obid=%d' % (day[0], day[1], day[2], obid)
    unit1_counts = [ ob_data.get(i, 0) for i in xrange(1440) ]
    daily_total = sum(unit1_counts)

    # insert daily_record_count
    cur = conn.cursor()
    with closing(cur):
        sql = 'INSERT INTO daily_record_count(observation_id, day, daily_total_count) VALUES (%s, %s, %s) RETURNING id;'

        date = datetime.date(day[0], day[1], day[2])
        cur.execute(sql, (obid, date, daily_total))

        # get last inserted daily_record_count.id
        drc_id = cur.fetchone()[0]
        # print '    daily_record_count.id=%d' % drc_id

    # save unit values
    all_unit = (1, 5, 10, 30, 60, 360, 720)
    for unit in all_unit:
        save_units(conn, drc_id, unit, unit1_counts)
    # print '    ok'
    conn.commit()


def log(msg):
    print '%s %s' % (datetime.datetime.now(), msg)


@click.command()
@click.option('-c', '--config-file')
def main(config_file):
    if not config_file or not os.path.exists(config_file):
        print 'missing config file: %s' % config_file
        sys.exit(-1)

    now = datetime.datetime.now()
    today = datetime.datetime(now.year, now.month, now.day, 0, 0, 0)

    with open(config_file, 'rb') as fh:
        config = json.load(fh)

    conn = open_connection(config)
    with closing(conn):
        saved = 0
        # create observation_id lists
        all_obids = fetch_observation_id_list(conn)
        print 'all_obids=%d' % len(all_obids)

        # determine start point
        start_day = determine_start_day(conn)
        day = start_day
        dt_day = datetime.datetime(day[0], day[1], day[2], 0, 0, 0)
        while dt_day < today:
            log('%04d-%02d-%02d start' % (day[0], day[1], day[2]))
            t1 = time.time()
            sql = 'select observation_id, extract(hour from created) * 60 + extract(minute from created) as min_index, count(id) from record where created between %s and %s + interval \'1 day\' group by observation_id, min_index order by observation_id, min_index;'

            cur = conn.cursor()
            reported_obids = set([])
            with closing(cur):
                cur.execute(sql, (dt_day, dt_day))
                current_obid = None
                ob_data = dict()
                for obid, min_index, count in cur:
                    if current_obid is None:
                        current_obid = obid
                    else:
                        if current_obid != obid:
                            save_daily_report(day, conn, current_obid, ob_data)
                            reported_obids.add(current_obid)
                            current_obid = obid
                            ob_data = dict()
                            saved += 1
                            if saved % 1000 == 0:
                                log('    %04d-%02d-%02d: %d, %.3fsec passed' % (day[0], day[1], day[2], saved, time.time() - t1))
                    ob_data[min_index] = count

                if 0 < len(ob_data):
                    save_daily_report(day, conn, current_obid, ob_data)
                    reported_obids.add(current_obid)
                    saved += 1

                for obid in all_obids:
                    if obid not in reported_obids:
                        # not appeared: create report with all zero
                        save_daily_report(day, conn, obid, dict())
                        saved += 1
                        if saved % 1000 == 0:
                                log('    %04d-%02d-%02d: %d, %.3fsec passed' % (day[0], day[1], day[2], saved, time.time() - t1))
                        # print '    @@@ saved not record this day: obid=%d' % obid

            t2 = time.time()
            print 'finisehd: %04d-%02d-%02d: %d observations, %.3fsec' % (
                day[0], day[1], day[2], len(all_obids), t2 - t1)

            day = next_day(*day)
            dt_day = datetime.datetime(day[0], day[1], day[2], 0, 0, 0)


if __name__ == '__main__':
    main()
