#!/usr/bin/env python3
#
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import argparse
import asyncio
import csv
import io
import itertools
import json
import re
import sys
import time
from concurrent import futures

import aiopg
import asyncpg
import numpy as np
import postgresql
import psycopg
import psycopg2
import psycopg2.extras
import uvloop


def _chunks(iterable, n):
    i = 0

    def _ctr(_):
        nonlocal i
        k = i // n
        i += 1
        return k
    for _, g in itertools.groupby(iterable, _ctr):
        yield g


def psycopg_connect(args):
    conn = psycopg2.connect(user=args.pguser, host=args.pghost,
                            port=args.pgport)
    return conn

def psycopg2_connect(args):
    conn = psycopg2.connect(user=args.pguser, host=args.pghost,
                            port=args.pgport)
    return conn

def psycopg2_execute(conn, query, args):
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(query, args)
    return len(cur.fetchall())

def psycopg_execute(conn, query, args):
    cur = conn.cursor(row_factory=psycopg.rows.dict_row)
    cur.execute(query, args)
    return len(cur.fetchall())

def psycopg2_copy(conn, query, args):
    rows, copy = args[:2]
    f = io.StringIO()
    writer = csv.writer(f, delimiter='\t')
    for row in rows:
        writer.writerow(row)
    f.seek(0)
    cur = conn.cursor()
    cur.copy_from(f, copy['table'], columns=copy['columns'])
    conn.commit()
    return cur.rowcount

def psycopg_copy(conn, query, args):
    rows, copy = args[:2]
    f = io.StringIO()
    writer = csv.writer(f, delimiter='\t')
    for row in rows:
        writer.writerow(row)
    f.seek(0)
    with conn.cursor() as cur:
        with cur.copy(query) as copy:
            copy.write(f.read())
        conn.commit()
        return cur.rowcount

def psycopg2_executemany(conn, query, args):
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.executemany(query, args)
    return len(args)

def psycopg_executemany(conn, query, args):
    with conn.cursor() as cur:
        cur.executemany(query, args)
    return len(args)


def pypostgresql_connect(args):
    conn = postgresql.open(user=args.pguser, host=args.pghost,
                           port=args.pgport)
    return conn


def pypostgresql_execute(conn, query, args):
    stmt = conn.prepare(query)
    return len(list(stmt.rows(*args)))


async def aiopg_connect(args):
    conn = await aiopg.connect(user=args.pguser, host=args.pghost,
                               port=args.pgport)
    return conn


async def aiopg_execute(conn, query, args):
    cur = await conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    await cur.execute(query, args)
    rv = len(await cur.fetchall())
    cur.close()
    return rv


async def _aiopg_executemany(cursor, query, rows):
    for batch in _chunks(rows, n=100):
        sqls = [cursor.mogrify(query, args) for args in batch]
        await cursor.execute(b";".join(sqls))
    return len(rows)


async def aiopg_executemany(conn, query, rows):
    cur = await conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    rv = await _aiopg_executemany(cur, query, rows)
    cur.close()
    return rv


aiopg_tuples_connect = aiopg_connect


async def aiopg_tuples_execute(conn, query, args):
    cur = await conn.cursor()
    await cur.execute(query, args)
    rv = len(await cur.fetchall())
    cur.close()
    return rv


async def aiopg_tuples_executemany(conn, query, rows):
    cur = await conn.cursor()
    rv = await _aiopg_executemany(cur, query, rows)
    cur.close()
    return rv


async def asyncpg_connect(args):
    conn = await asyncpg.connect(user=args.pguser, host=args.pghost,
                                 port=args.pgport)
    return conn


async def async_psycopg_connect(args):
    conn = await psycopg.AsyncConnection.connect(user=args.pguser, host=args.pghost,
                                 port=args.pgport)
    return conn


async def asyncpg_execute(conn, query, args):
    return len(await conn.fetch(query, *args))


async def async_psycopg_execute(conn, query, args):
    cur = conn.cursor(row_factory=psycopg.rows.dict_row)
    await cur.execute(query, args)
    return len(await cur.fetchall())


async def asyncpg_executemany(conn, query, args):
    await conn.executemany(query, args)
    return len(args)


async def async_psycopg_executemany(conn, query, args):
    async with conn.cursor() as cur:
        await cur.executemany(query, args)
    return len(args)


async def asyncpg_copy(conn, query, args):
    rows, copy = args[:2]
    result = await conn.copy_records_to_table(
        copy['table'], columns=copy['columns'], records=rows)
    cmd, _, count = result.rpartition(' ')
    return int(count)


async def async_psycopg_copy(conn, query, args):
    rows, copy = args[:2]
    f = io.StringIO()
    writer = csv.writer(f, delimiter='\t')
    for row in rows:
        writer.writerow(row)
    f.seek(0)
    
    async with conn.cursor() as cur:
        async with cur.copy(query) as copy:
            await copy.write(f.read())
        await conn.commit()
        return cur.rowcount


async def worker(executor, eargs, start, duration, timeout):
    queries = 0
    rows = 0
    latency_stats = np.zeros((timeout * 100,))
    min_latency = float('inf')
    max_latency = 0.0

    while time.monotonic() - start < duration:
        req_start = time.monotonic()
        rows += await executor(*eargs)
        req_time = round((time.monotonic() - req_start) * 1000 * 100)

        if req_time > max_latency:
            max_latency = req_time
        if req_time < min_latency:
            min_latency = req_time
        latency_stats[req_time] += 1
        queries += 1

    return queries, rows, latency_stats, min_latency, max_latency


def sync_worker(executor, eargs, start, duration, timeout):
    queries = 0
    rows = 0
    latency_stats = np.zeros((timeout * 100,))
    min_latency = float('inf')
    max_latency = 0.0

    while time.monotonic() - start < duration:
        req_start = time.monotonic()
        rows += executor(*eargs)
        req_time = round((time.monotonic() - req_start) * 1000 * 100)

        if req_time > max_latency:
            max_latency = req_time
        if req_time < min_latency:
            min_latency = req_time
        latency_stats[req_time] += 1
        queries += 1

    return queries, rows, latency_stats, min_latency, max_latency


async def runner(args, connector, executor, copy_executor, batch_executor,
                 is_async, arg_format, query, query_args, setup, teardown):

    timeout = args.timeout * 1000
    concurrency = args.concurrency

    if arg_format == 'python':
        query = re.sub(r'\$\d+', '%s', query)

    is_copy = query.startswith('COPY ')
    is_batch = query_args and isinstance(query_args[0], dict)

    if is_copy:
        if copy_executor is None:
            raise RuntimeError('COPY is not supported for {}'.format(executor))
        executor = copy_executor

        match = re.match('COPY (\w+)\s*\(\s*((?:\w+)(?:,\s*\w+)*)\s*\)', query)
        if not match:
            raise RuntimeError('could not parse COPY query')

        query_info = query_args[0]
        query_args[0] = [query_info['row']] * query_info['count']
        query_args.append({
            'table': match.group(1),
            'columns': [col.strip() for col in match.group(2).split(',')]
        })
    elif is_batch:
        if batch_executor is None:
            raise RuntimeError('batch is not supported for {}'.format(executor))
        executor = batch_executor

        query_info = query_args[0]
        query_args = [query_info['row']] * query_info['count']

    conns = []

    for i in range(concurrency):
        if is_async:
            conn = await connector(args)
        else:
            conn = connector(args)
        conns.append(conn)

    async def _do_run(run_duration):
        start = time.monotonic()

        tasks = []

        if is_async:
            # Asyncio driver
            for i in range(concurrency):
                task = worker(executor, [conns[i], query, query_args],
                              start, run_duration, timeout)
                tasks.append(task)

            results = await asyncio.gather(*tasks)
        else:
            # Sync driver
            with futures.ThreadPoolExecutor(max_workers=concurrency) as e:
                for i in range(concurrency):
                    task = e.submit(sync_worker, executor,
                                    [conns[i], query, query_args],
                                    start, run_duration, timeout)
                    tasks.append(task)

                results = [fut.result() for fut in futures.wait(tasks).done]

        end = time.monotonic()

        return results, end - start

    if setup:
        admin_conn = await asyncpg.connect(user=args.pguser, host=args.pghost,
                                           port=args.pgport)
        await admin_conn.execute(setup)

    try:
        try:
            if args.warmup_time:
                await _do_run(args.warmup_time)

            results, duration = await _do_run(args.duration)
        finally:
            for conn in conns:
                if is_async:
                    await conn.close()
                else:
                    conn.close()

        min_latency = float('inf')
        max_latency = 0.0
        queries = 0
        rows = 0
        latency_stats = None

        for result in results:
            t_queries, t_rows, t_latency_stats, t_min_latency, t_max_latency =\
                result
            queries += t_queries
            rows += t_rows
            if latency_stats is None:
                latency_stats = t_latency_stats
            else:
                latency_stats = np.add(latency_stats, t_latency_stats)
            if t_max_latency > max_latency:
                max_latency = t_max_latency
            if t_min_latency < min_latency:
                min_latency = t_min_latency

        if is_copy:
            copyargs = query_args[-1]

            rowcount = await admin_conn.fetchval('''
                SELECT
                    count(*)
                FROM
                    "{tabname}"
            '''.format(tabname=copyargs['table']))

            print(rowcount, file=sys.stderr)

            if rowcount < len(query_args[0]) * queries:
                raise RuntimeError(
                    'COPY did not insert the expected number of rows')

        data = {
            'queries': queries,
            'rows': rows,
            'duration': duration,
            'min_latency': min_latency,
            'max_latency': max_latency,
            'latency_stats': latency_stats.tolist(),
            'output_format': args.output_format
        }

    finally:
        if teardown:
            await admin_conn.execute(teardown)

    print(json.dumps(data))


def die(msg):
    print('fatal: {}'.format(msg), file=sys.stderr)
    sys.exit(1)


if __name__ == '__main__':
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    loop = asyncio.get_event_loop()

    parser = argparse.ArgumentParser(
        description='async pg driver benchmark [concurrent]')
    parser.add_argument(
        '-C', '--concurrency', type=int, default=10,
        help='number of concurrent connections')
    parser.add_argument(
        '-D', '--duration', type=int, default=30,
        help='duration of test in seconds')
    parser.add_argument(
        '--timeout', default=2, type=int,
        help='server timeout in seconds')
    parser.add_argument(
        '--warmup-time', type=int, default=5,
        help='duration of warmup period for each benchmark in seconds')
    parser.add_argument(
        '--output-format', default='text', type=str,
        help='output format', choices=['text', 'json'])
    parser.add_argument(
        '--pghost', type=str, default='127.0.0.1',
        help='PostgreSQL server host')
    parser.add_argument(
        '--pgport', type=int, default=5432,
        help='PostgreSQL server port')
    parser.add_argument(
        '--pguser', type=str, default='postgres',
        help='PostgreSQL server user')
    parser.add_argument(
        'driver', help='driver implementation to use',
        choices=['aiopg', 'aiopg-tuples', 'asyncpg', 'psycopg2', 'psycopg3', 'psycopg3[async]', 'postgresql'])
    parser.add_argument(
        'queryfile', help='file to read benchmark query information from')

    args = parser.parse_args()

    if args.queryfile == '-':
        querydata_text = sys.stdin.read()
    else:
        with open(args.queryfile, 'rt') as f:
            querydata_text = f.read()

    querydata = json.loads(querydata_text)

    query = querydata.get('query')
    if not query:
        die('missing "query" in query JSON')

    query_args = querydata.get('args')
    if not query_args:
        query_args = []

    setup = querydata.get('setup')
    teardown = querydata.get('teardown')
    if setup and not teardown:
        die('"setup" is present, but "teardown" is missing in query JSON')

    copy_executor = None
    batch_executor = None

    if args.driver == 'aiopg':
        if query.startswith('COPY '):
            connector, executor, copy_executor = \
                psycopg_connect, psycopg_execute, psycopg_copy
            is_async = False
        else:
            connector, executor, batch_executor = \
                aiopg_connect, aiopg_execute, aiopg_executemany
            is_async = True
        arg_format = 'python'
    elif args.driver == 'aiopg-tuples':
        if query.startswith('COPY '):
            connector, executor, copy_executor = \
                psycopg_connect, psycopg_execute, psycopg_copy
            is_async = False
        else:
            connector, executor, batch_executor = \
                aiopg_tuples_connect, aiopg_tuples_execute, \
                aiopg_tuples_executemany
            is_async = True
        arg_format = 'python'
    elif args.driver == 'asyncpg':
        connector, executor, copy_executor, batch_executor = \
            asyncpg_connect, asyncpg_execute, asyncpg_copy, asyncpg_executemany
        is_async = True
        arg_format = 'native'
    elif args.driver == 'psycopg2':
        connector, executor, copy_executor, batch_executor = \
            psycopg2_connect, psycopg2_execute, psycopg2_copy, psycopg2_executemany
        is_async = False
        arg_format = 'python'
    elif args.driver == 'psycopg3':
        connector, executor, copy_executor, batch_executor = \
            psycopg_connect, psycopg_execute, psycopg_copy, psycopg_executemany
        is_async = False
        arg_format = 'python'
    elif args.driver == 'psycopg3[async]':
        connector, executor, copy_executor, batch_executor = \
            async_psycopg_connect, async_psycopg_execute, async_psycopg_copy, async_psycopg_executemany
        is_async = True
        arg_format = 'python'
    elif args.driver == 'postgresql':
        connector, executor = pypostgresql_connect, pypostgresql_execute
        is_async = False
        arg_format = 'native'
    else:
        raise ValueError('unexpected driver: {!r}'.format(args.driver))

    runner_coro = runner(args, connector, executor, copy_executor,
                         batch_executor, is_async,
                         arg_format, query, query_args, setup, teardown)
    loop.run_until_complete(runner_coro)
