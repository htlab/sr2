# -*- coding: utf-8 -*-
import os.path
import sys
import json
import time
import datetime
import commands
import traceback
import functools
from contextlib import closing

import click
import psycopg2
#from apscheduler.scheduler import Scheduler
from apscheduler.schedulers.blocking import BlockingScheduler



def open_connection(config):
    pg_host = config['host']
    pg_db = config['db']
    pg_user = config['user']
    pg_pw = config['pw']

    connect_info = 'host=%s dbname=%s user=%s password=%s' % (
        pg_host, pg_db, pg_user, pg_pw)
    return psycopg2.connect(connect_info)


def get_first(conn, sql):
    t1 = time.time()
    cur = conn.cursor()
    with closing(cur):
        cur.execute(sql)
        try:
            row = cur.fetchone()
        except:
            traceback.print_exc()
            return None
        else:
            t2 = time.time()
            print '%.1fmsec: %s' % (1000 * (t2 - t1), sql)
            return row[0]


def get_disk_usage(pg_data_dir):
    output = commands.getoutput('du -sk %s' % pg_data_dir)
    import pprint
    pprint.pprint(output)
    data_kb, dir_name = output.split('\t')
    return int(data_kb)



def measure_stat(conn, config):
    t1 = time.time()
    pg_data_dir = config['pg_data_dir']
    print '----------------------------'
    total_record_count = get_first(conn, 'select count(id) from record;')
    total_observation_count = get_first(conn, 'select count(id) from observation;')
    total_large_object_count = get_first(conn, 'select count(id) from large_object;')
    total_transducer_count = get_first(conn, 'select count(id) from transducer;')
    average_transducers_per_observation = float(total_transducer_count) / total_observation_count
    total_export_count = get_first(conn, 'select count(id) from export;')

    total_disk_usage_kbytes = get_disk_usage(pg_data_dir)

    recent_1min_error_log_count = get_first(conn, 'select count(id) from event_log where log_level = 4 and logged_at between now() - interval \'1 minute\' and now();');
    recent_1min_warn_log_count = get_first(conn, 'select count(id) from event_log where log_level = 3 and logged_at between now() - interval \'1 minute\' and now();');
    recent_1min_record_count = get_first(conn, 'select count(id) from record where created between now() - interval \'1 minute\' and now();');

    t2 = time.time()
    print 'Total: %.3fsec' % (t2 - t1)

    return dict(
        total_record_count=total_record_count,
        total_observation_count=total_observation_count,
        total_large_object_count=total_large_object_count,
        total_transducer_count=total_transducer_count,
        average_transducers_per_observation=average_transducers_per_observation,
        total_export_count=total_export_count,
        total_disk_usage_kbytes=total_disk_usage_kbytes,
        recent_1min_error_log_count=recent_1min_error_log_count,
        recent_1min_warn_log_count=recent_1min_warn_log_count,
        recent_1min_record_count=recent_1min_record_count
    )


def record_stat(config):
    try:
        print '%s record_stat() start' % datetime.datetime.now()
        conn = open_connection(config)

        # insert_fields = (
        #     ('total_record_count', '%d'),
        #     ('total_observation_count', '%d'),
        #     ('total_large_object_count', '%d'),
        #     ('total_transducer_count', '%d'),
        #     ('average_transducers_per_observation', '%f'),
        #     ('total_export_count', '%d'),
        #     ('total_disk_usage_kbytes', '%d'),
        #     ('recent_1min_warn_log_count', '%d'),
        #     ('recent_1min_error_log_count', '%d'),
        #     ('recent_1min_record_count', '%d'),
        #     ('created', '%s')
        # )
        insert_fields = (
            ('total_record_count', '%s'),
            ('total_observation_count', '%s'),
            ('total_large_object_count', '%s'),
            ('total_transducer_count', '%s'),
            ('average_transducers_per_observation', '%s'),
            ('total_export_count', '%s'),
            ('total_disk_usage_kbytes', '%s'),
            ('recent_1min_warn_log_count', '%s'),
            ('recent_1min_error_log_count', '%s'),
            ('recent_1min_record_count', '%s'),
            ('created', '%s')
        )

        with closing(conn):
            stat = measure_stat(conn, config)
            stat['created'] = datetime.datetime.now()

            comma_fields = ','.join([ f[0] for f in insert_fields ])
            comma_placeholders = ','.join([ f[1] for f in insert_fields ])
            insert_sql = 'INSERT INTO sox_recorder_stat(%s) VALUES (%s);' % (comma_fields, comma_placeholders)
            print insert_sql

            insert_sql_args = tuple([ stat[k[0]] for k in insert_fields ])
            print insert_sql_args

            cur = conn.cursor()
            with closing(cur):
                cur.execute(insert_sql, insert_sql_args)
            conn.commit()

            # print 'record_stat(): %s inserted' % stat['created']
            print '%s record_start() ok' % datetime.datetime.now()
    except:
        traceback.print_exc()


@click.command()
@click.option('-c', '--config', help='config json file')
def main(config):
    if not config or not os.path.exists(config):
        print 'missing config (-c/--config): %s' % config
        sys.exit(-1)

    with open(config, 'rb') as fh:
        config = json.load(fh)

    keys = 'pg_data_dir host db user pw'.split(' ')
    for key in keys:
        assert key in config

    task = functools.partial(record_stat, config)
    task()

    scheduler = BlockingScheduler(standaline=True, coalesce=True)
    scheduler.add_job(task, 'interval', seconds=60)
    scheduler.start()


if __name__ == '__main__':
    main()
