# -*- coding: utf-8 -*-
# 簡易的なエクスポートの実装
import re
import sys
import getpass
import datetime
import time
import cStringIO as StringIO
import gzip
from contextlib import closing
import os
import os.path
import collections
import multiprocessing
import traceback

import simplejson as json
import psycopg2
import click

TYPE_STR = 1
TYPE_INT = 2
TYPE_FLOAT = 3
TYPE_DECIMAL = 4
TYPE_LO = 5

CSV_EXPORT_ENC = 'cp932'


_cache = dict()
_cache_loids = collections.deque([])
_max_mem_loid = 5000

_encoding = None
_columns = None

pat_dt = re.compile(r'\A(\d{4})-(\d\d)-(\d\d) (\d\d):(\d\d):(\d\d)\Z')


def parse_dt(dt):
    if dt is None:
        return None
    m = pat_dt.match(dt)
    if not m:
        return None
    return datetime.datetime(*[ int(n) for n in m.groups() ])


def str_dt(dt):
    return dt.strftime('%Y-%m-%d %H:%M:%S')


def cache_get(loid):
    if loid in _cache:
        return _cache[loid]
    else:
        raise KeyError()


def cache_set(loid, value):
    global _cache, _cache_loids, _max_mem_loid
    if loid in _cache:
        return

    if len(_cache_loids) == _max_mem_loid:
        remove_loid = _cache_loids.popleft()
        del _cache[remove_loid]

    _cache_loids.append(loid)
    _cache[loid] = value


def gunzip(content):
    with closing(StringIO.StringIO(content)) as sio:
        with closing(gzip.GzipFile('dummy_fname', 'rb', fileobj=sio)) as gz:
            return gz.read()


# def get_large_object(pg_conn, lo_id):
#     with closing(pg_conn.cursor()) as cursor:
#         sql = 'SELECT content, is_gzipped FROM large_object WHERE id = %s;'
#         cursor.execute(sql, (lo_id,))
#         row = cursor.fetchone()
#         if row is None:
#             return None
#
#         content, is_gzipped = row
#         return gunzip(content) if is_gzipped else content


def resolve_large_objects(pg_conn, lo_ids):
    if len(lo_ids) == 0:
        return dict()
    ret = dict()
    lo_ids = tuple(lo_ids)

    not_in_cache = []

    for loid in lo_ids:
        try:
            ret[loid] = cache_get(loid)
        except KeyError:
            not_in_cache.append(loid)

    if len(not_in_cache) == 0:
        return ret

    with closing(pg_conn.cursor()) as cursor:
        placeholders = '(' + ','.join(['%s'] * len(not_in_cache)) + ')'
        sql = 'SELECT id, content, is_gzipped FROM large_object WHERE id IN %s' % placeholders
        # cursor.execute(sql, lo_ids)
        cursor.execute(sql, not_in_cache)
        for loid, content, is_gzipped in cursor:
            # ret[loid] = gunzip(content) if is_gzipped else content
            content = gunzip(content) if is_gzipped else content
            ret[loid] = content
            cache_set(loid, content)
    return ret


def get_transducer_values(pg_conn, tid2tname, recbuf, columns):
    if len(recbuf) == 0:
        return dict()
    ret = dict()
    r_ids = tuple([ r['id'] for r in recbuf ])

    lo_ids = []

    with closing(pg_conn.cursor()) as cursor:
        placeholders = '(' + ','.join(['%s'] * len(recbuf)) + ')'

        sql = '''
        SELECT
            record_id,
            value_type,
            transducer_id,
            string_value,
            int_value,
            float_value,
            decimal_value,
            large_object_id,
            transducer_timestamp
        FROM
            transducer_raw_value
        WHERE
            record_id IN %s
        ;
        ''' % placeholders
        cursor.execute(sql, r_ids)

        for row in cursor:
            rid, vt, tid, v_str, v_int, v_float, v_decimal, v_loid, tdr_ts = row
            tname = tid2tname[tid]

            if vt == TYPE_LO:
                # value = get_large_object(pg_conn, v_loid)
                columns[tname] = True
                value = None
                lo_ids.append( (rid, tname, v_loid) )
            else:
                columns[tname] = columns.get(tname, False) or False
                if vt == TYPE_STR:
                    value = v_str
                elif vt == TYPE_INT:
                    value = v_int
                elif vt == TYPE_FLOAT:
                    value = v_float
                elif vt == TYPE_DECIMAL:
                    value = str(v_decimal)

            if rid not in ret:
                ret[rid] = dict()

            ret[rid][tname] = value

    # resolve large object
    query_loids = set([ loid for rid, tname, loid in lo_ids ])
    loid2content = resolve_large_objects(pg_conn, query_loids)
    for rid, tname, loid in lo_ids:
        ret[rid][tname] = loid2content.get(loid, None)

    return ret


def fill_recbuf(pg_conn, tid2tname, recbuf, columns):
    rid2tdrvalues = get_transducer_values(pg_conn, tid2tname, recbuf, columns)
    for record in recbuf:
        rec_tdrvalues = rid2tdrvalues.get(record['id'], {})
        for tdr_name, tdr_value in rec_tdrvalues.iteritems():
            record[tdr_name] = tdr_value


def make_json_line(dict_data):
    return json.dumps(dict_data) + '\n'


def export_json(pg_conn, sox_server, sox_node, out_file, from_time, until_time):
    """
    return value is 3 value tuple
    (
        found(bool),
        {col => True(if_largeobject), col2 => False(if not lo)},
        rows
    )
    """

    meta_f = out_file + '.meta'

    # observation ID を確定する
    with closing(pg_conn.cursor()) as cursor:
        sql = 'SELECT id FROM observation WHERE sox_server = %s AND sox_node = %s;'
        cursor.execute(sql, (sox_server, sox_node))
        result = cursor.fetchone()
        if result is None:
            return (False, dict(), 0)
        else:
            ob_id = result[0]

    # print 'got obid=%d for node=%s' % (ob_id, sox_node)

    # すべてのtransducerをリスト
    tid2tname = dict()
    with closing(pg_conn.cursor()) as cursor:
        sql = 'SELECT id, transducer_id FROM transducer WHERE observation_id = %s;'
        cursor.execute(sql, (ob_id,))
        for t_id, t_name in cursor:
            tid2tname[t_id] = t_name

    transducers = sorted(tid2tname.values())
    # print 'got transducers: N=%d' % len(transducers)

    types = {
        1: 'string',
        2: 'integer',
        3: 'float',
        4: 'decimal',
        5: 'large_object'
    }

    # clickにETAや進捗を出させたいのでヒット数を数える
    with closing(pg_conn.cursor()) as cursor:
        conditions = ['(observation_id = %s)']
        args = [ob_id]
        if from_time:
            conditions.append('(%s <= created)')
            args.append(from_time)
        if until_time:
            conditions.append('(created < %s)')
            args.append(until_time)
        cond = ' AND '.join(conditions)
        sql = 'SELECT COUNT(*) FROM record WHERE %s;' % cond
        # cursor.execute(sql, (ob_id,))
        cursor.execute(sql, args)
        (row_count,) = cursor.fetchone()

    # すでに全部exportずみかテストする
    if os.path.exists(meta_f) and os.path.exists(out_file):
        with open(meta_f, 'rb') as fh:
            meta = json.load(fh)
            meta_ft = meta.get('from_time', 'old_version')
            meta_ut = meta.get('until_time', 'old_version')

        if meta_ft != 'old_version':
            is_same_time = (parse_dt(meta_ft) == from_time) and (parse_dt(meta_ut) == until_time)
            if meta['row_count'] == row_count and is_same_time:
                # すでにこの設定のデータはexportずみ
                print '  (JSON) %s => found already exported file, rows=%d' % (sox_node, row_count)
                return (True, meta['columns'], row_count)

    # transducer_raw_value についてクエリする
    columns = dict()
    pool = multiprocessing.Pool(multiprocessing.cpu_count())
    chunks = 20
    with closing(pool):
        with closing(pg_conn.cursor()) as cursor:
            conditions = ['(observation_id = %s)']
            args = [ob_id]

            if from_time:
                conditions.append('(%s <= created)')
                args.append(from_time)

            if until_time:
                conditions.append('(created < %s)')
                args.append(until_time)

            cond = ' AND '.join(conditions)
            sql = '''
            SELECT
                id, is_parse_error, created
            FROM
                record
            WHERE
                %s
            ORDER BY
                created ASC;
            ''' % cond
            cursor.execute(sql, tuple(args))
            record = None
            with open(out_file, 'wb') as fh:
                with click.progressbar(
                        length=row_count, label='  (JSON) %s' % sox_node,
                        show_eta=True, show_percent=True, show_pos=True) as bar:

                    recbuf = []
                    n_flush = 1000
                    for rid, is_parse_error, created in cursor:
                        record = {
                            'id': rid,
                            'is_parse_error': is_parse_error,
                            'created': created.strftime('%Y-%m-%d %H:%M:%S')
                        }
                        recbuf.append(record)
                        if n_flush <= len(recbuf):
                            fill_recbuf(pg_conn, tid2tname, recbuf, columns)

                            for rec_jsonline in pool.imap(make_json_line, recbuf, chunksize=chunks):
                                fh.write(rec_jsonline)
                                bar.update(1)

                            recbuf = []

                    if 0 < len(recbuf):
                        fill_recbuf(pg_conn, tid2tname, recbuf, columns)
                        for rec_jsonline in pool.imap(make_json_line, recbuf, chunksize=chunks):
                            fh.write(rec_jsonline)
                            bar.update(1)

    meta = dict(
        sox_server=sox_server,
        sox_node=sox_node,
        columns=columns,
        row_count=row_count,
        from_time=(str_dt(from_time) if from_time else None),
        until_time=(str_dt(until_time) if until_time else None)
    )

    with open(meta_f, 'wb') as fh:
        fh.write(json.dumps(meta))

    return (True, columns, row_count)


def conv_json_line(line):
    global _columns, _encoding
    line_data = json.loads(line)
    values = [ line_data.get(col, None) for col in _columns ]
    return make_csv_line(values, encoding=_encoding)


def convert_json2csv(
        json_lines_file, out_file, node, columns,
        drop_large=True, n_lines=None, enc='cp932'):

    global _pool, _columns, _encoding

    csv_meta_f = out_file + '.meta'
    json_meta_f = json_lines_file + '.meta'
    if os.path.exists(csv_meta_f) and os.path.exists(out_file):
        with open(csv_meta_f, 'rb') as fh, open(json_meta_f, 'rb') as jfh:
            csv_meta = json.load(fh)
            json_meta = json.load(jfh)

        if csv_meta == json_meta:
            print '  (CSV)  %s => found already exported csv!' % node
            return  # already exported!

    _encoding = enc

    if drop_large:
        columns = [ col for col, using_lo in columns.items() if not using_lo ]
    else:
        columns = [ col for col, using_lo in columns.items() ]

    _columns = ['id', 'created', 'is_parse_error']
    _columns.extend(columns)
    # _columns = columns

    pool = multiprocessing.Pool(multiprocessing.cpu_count())
    with closing(pool):
        with open(json_lines_file, 'rb') as in_fh, open(out_file, 'wb') as out_fh:
            out_fh.write(make_csv_line(_columns, encoding=enc))
            with click.progressbar(
                    length=n_lines,
                    label='  (CSV)  %s' % node,
                    show_eta=True,
                    show_pos=True, show_percent=True) as bar:
                # for line in in_fh:
                #     line_data = json.loads(line)
                #     values = [ line_data.get(col, None) for col in columns ]
                #     out_fh.write(make_csv_line(values, encoding=enc))
                #     bar.update(1)
                for csv_line in pool.imap(conv_json_line, in_fh, chunksize=20):
                    out_fh.write(csv_line)
                    bar.update(1)

    with open(csv_meta_f, 'wb') as fh, open(json_meta_f, 'rb') as jfh:
        fh.write(jfh.read())


def escape_csv_value(v, encoding):
    if isinstance(v, unicode):
        try:
            v = v.encode(encoding)
        except:
            v = v.encode('utf-8')

    if isinstance(v, (str, unicode)):
        if '"' in v:
            return '"%s"' % v.replace('"', '""')
        else:
            return v
    else:
        return str(v)


def make_csv_line(values, encoding):
    return ','.join([ escape_csv_value(v, encoding) for v in values ]) + '\n'


def get_nodes(f):
    ret = []
    added = set([])
    with open(f, 'rb') as fh:
        for line in fh:
            line = line.replace('\n', '').decode('utf-8')
            if line == '':
                continue

            if line in added:
                continue

            ret.append(line)
            added.add(line)
    return ret


@click.command()
@click.option('-h', '--host', default='localhost', help='postgres host (default: localhost)')
@click.option('-d', '--db', default='sr2', help='postgres db (default: sr2)')
@click.option('-u', '--user', default='sr2', help='postgres user (default: sr2)')
@click.option('-s', '--sox-server', default='sox.ht.sfc.keio.ac.jp', help='tareget sox server (default: sox.ht)')
@click.option('-n', '--node-list-file', help='node list file')
@click.option('-o', '--out-dir', help='output dir')
@click.option('-c', '--no-csv', type=bool, default=False, help='no csv file generation (default: False)')
@click.option('-l', '--no-large-in-csv', type=bool, default=True, help='no large object in csv (default: True)')
@click.option('-y', '--no-confirm', type=bool, default=False, help='no confirmation (default: False)')
@click.option('-f', '--from-time', default='', help='from time(optional) (format: "2016-10-01 12:34:56")')
@click.option('-u', '--until-time', default='', help='until time(optional) (format: "2016-10-10 00:00:00")')
def main(
        host, db, user, sox_server, node_list_file, out_dir,
        no_csv, no_large_in_csv, no_confirm, from_time, until_time):
    if not node_list_file:
        print 'ERROR: parameter -n/--node-list-file not specified'
        sys.exit(-1)
    elif not os.path.exists(node_list_file):
        print 'ERROR: node list file \'%s\' not existing' % node_list_file
        sys.exit(-1)

    if not out_dir:
        print 'ERROR: parameter -o/--out-dir not specified'
        sys.exit(-1)

    if not os.path.exists(out_dir):
        try:
            os.makedirs(out_dir)
        except:
            print 'ERROR: could not create out_dir: %s' % out_dir
            sys.exit(-1)
        else:
            print 'created out_dir: %s' % out_dir

    from_time = parse_dt(from_time)
    until_time = parse_dt(until_time)

    print 'Time-from: %s' % ('Beginning' if from_time is None else from_time.strftime('%Y-%m-%d %H:%M:%S'))
    print 'Time-until: %s' % ('Latest' if until_time is None else until_time.strftime('%Y-%m-%d %H:%M:%S'))
    print 'Output dir: %s' % out_dir

    print 'SOX Server: %s' % sox_server
    nodes = get_nodes(node_list_file)
    print 'SOX Nodes: File=%s, N=%d' % (node_list_file, len(nodes))

    print 'PostgreSQL: Host=%s, DB=%s, User=%s' % (host, db, user)
    print 'PostgreSQL Password: '
    pw = getpass.getpass()
    if pw == '':
        print 'ERROR: Password was empty'
        sys.exit(-1)

    print 'CSV Export: %s' % ('No' if no_csv else 'Yes')

    if not no_confirm:
        print 'OK?(y/n): '
        if raw_input().lower()[0] != 'y':
            print 'Abort!'
            sys.exit(-1)

    print 'Starting.'

    dt_start = datetime.datetime.now()
    db_exports = []
    db_csv_conv = []
    not_found = []
    # empty_nodes = []
    node_rows = []

    pg_conn = psycopg2.connect(host=host, database=db, user=user, password=pw)
    print 'Connected to PostgreSQL(host=%s)' % host
    with closing(pg_conn):
        for i, node in enumerate(nodes):
            print '(%d/%d) node=%s' % (i + 1, len(nodes), node)
            safenode = node.replace('/', '__slash__')
            basename = u'%s.json' % safenode
            json_out_f = os.path.join(out_dir, basename)
            t1 = time.time()
            found, columns, rows = export_json(pg_conn, sox_server, node, json_out_f, from_time, until_time)
            t2 = time.time()
            db_exports.append(t2 - t1)
            node_rows.append([node, rows])

            if not found:
                not_found.append( (sox_server, sox_node) )

            if found and not no_csv:
                csv_basename = u'%s.csv' % safenode
                csv_out_f = os.path.join(out_dir, csv_basename)
                t1 = time.time()
                convert_json2csv(
                    json_out_f, csv_out_f, node, columns, no_large_in_csv, rows)
                t2 = time.time()
                db_csv_conv.append(t2 - t1)

    dt_finish = datetime.datetime.now()
    sec_passed = (dt_finish - dt_start).total_seconds()

    print '------------------------------------------'
    print 'Time started:  %s' % str(dt_start)
    print 'Time finished: %s' % str(dt_finish)
    print 'Time passed: %.3fsec' % sec_passed
    if 0 < len(nodes):
        avg_export = sum(db_exports) / len(nodes)
        rows_counts = [ rows for node, rows in node_rows ]
        avg_rows = sum(rows_counts) / len(nodes)

        print 'Average exported rows: %.3f' % avg_rows
        print 'Average JSON lines export time: %.3fsec' % avg_export
        if not no_csv:
            avg_csv_conversion = sum(db_csv_conv) / len(nodes)
            print 'Average CSV conversion time: %.3fsec' % avg_csv_conversion

    if 0 < len(not_found):
        print 'WARNING: %d observations not found:' % len(not_found)
        for i, (sox_server, node) in enumerate(not_found):
            print '    %5d server=%s, node=%s' % (sox_server, node)

    node_rows_file = os.path.join(out_dir, 'node_rows.csv')
    with open(node_rows_file, 'wb') as fh:
        fh.write(make_csv_line(['node', 'records'], 'cp932'))
        for node, rows in sorted(node_rows, key=lambda x: x[1]):
            line = make_csv_line([node, rows], 'cp932')
            fh.write(line)

    meta = dict(
        time_start=str_dt(dt_start),
        time_finish=str_dt(dt_finish),
        time_passed_sec=sec_passed,
        sox_server=sox_server,
        n_nodes=len(nodes),
        node_list_file=node_list_file,
        nodes=nodes,
        out_dir=out_dir,
        postgres=dict(host=host, db=db, user=user)
    )
    meta_f = os.path.join(out_dir, '_meta.json')
    with open(meta_f, 'wb') as fh:
        json.dump(meta, fh, indent=4, sort_keys=True)


if __name__ == '__main__':
    main()
