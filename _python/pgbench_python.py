#!/usr/bin/env python3
#
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import argparse
import asyncio
from concurrent import futures
import json
import re
import sys
import time

import numpy as np
import uvloop

import aiopg
import asyncpg
import postgresql
import psycopg2
import psycopg2.extras


def psycopg_connect(args):
    conn = psycopg2.connect(user=args.pguser, host=args.pghost,
                            port=args.pgport)
    return conn


def psycopg_execute(conn, query, args):
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(query, args)
    return len(cur.fetchall())


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
    return len(await cur.fetchall())


aiopg_tuples_connect = aiopg_connect


async def aiopg_tuples_execute(conn, query, args):
    cur = await conn.cursor()
    await cur.execute(query, args)
    return len(await cur.fetchall())


async def asyncpg_connect(args):
    conn = await asyncpg.connect(user=args.pguser, host=args.pghost,
                                 port=args.pgport)
    return conn


async def asyncpg_execute(conn, query, args):
    return len(await conn.fetch(query, *args))


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


async def runner(args, connector, executor, is_async, arg_format, query,
                 query_args, setup, teardown):

    timeout = args.timeout * 1000
    concurrency = args.concurrency

    if arg_format == 'python':
        query = re.sub(r'\$\d+', '%s', query)

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
                              start, args.duration, timeout)
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
        if args.warmup_time:
            await _do_run(args.warmup_time)

        results, duration = await _do_run(args.duration)

    finally:
        if teardown:
            await admin_conn.execute(teardown)
            await admin_conn.close()

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
        t_queries, t_rows, t_latency_stats, t_min_latency, t_max_latency = \
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

    data = {
        'queries': queries,
        'rows': rows,
        'duration': duration,
        'min_latency': min_latency,
        'max_latency': max_latency,
        'latency_stats': latency_stats.tolist(),
        'output_format': args.output_format
    }

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
        choices=['aiopg', 'aiopg-tuples', 'asyncpg', 'psycopg', 'postgresql'])
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

    if args.driver == 'aiopg':
        connector, executor = aiopg_connect, aiopg_execute
        is_async = True
        arg_format = 'python'
    if args.driver == 'aiopg-tuples':
        connector, executor = aiopg_tuples_connect, aiopg_tuples_execute
        is_async = True
        arg_format = 'python'
    elif args.driver == 'asyncpg':
        connector, executor = asyncpg_connect, asyncpg_execute
        is_async = True
        arg_format = 'native'
    elif args.driver == 'psycopg':
        connector, executor = psycopg_connect, psycopg_execute
        is_async = False
        arg_format = 'python'
    elif args.driver == 'postgresql':
        connector, executor = pypostgresql_connect, pypostgresql_execute
        is_async = False
        arg_format = 'native'
    else:
        raise ValueError('unexpected driver: {!r}'.format(args.driver))

    runner_coro = runner(args, connector, executor, is_async, arg_format,
                         query, query_args, setup, teardown)
    loop.run_until_complete(runner_coro)
