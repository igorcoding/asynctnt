import argparse
import asyncio
import datetime
import logging
import math
import sys
from enum import Enum


class TypeBench(str, Enum):
    PING = "ping"
    CALL = "call"
    SELECT = "select"
    EVAL = "eval"
    SQL = "sql"
    REPLACE = "replace"
    UPDATE = "update"

    CHOICES = [
        PING, CALL, SELECT, EVAL, SQL, REPLACE, UPDATE
    ]


def main():
    logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)
    parser = argparse.ArgumentParser()
    parser.add_argument('--asynctnt', type=bool, default=False)
    parser.add_argument('--aiotnt', type=bool, default=False)
    parser.add_argument('--tarantool', type=bool, default=False)
    parser.add_argument('--uvloop', type=bool, default=False)
    parser.add_argument('-n', type=int, default=50000,
                        help='number of executed requests')
    parser.add_argument('-b', type=int, default=100, help='number of bulks')
    parser.add_argument('-t', type=str, default=TypeBench.SELECT, help='type of benchmark', choices=TypeBench.CHOICES)
    args = parser.parse_args()

    if args.uvloop:
        import uvloop
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

    loop = asyncio.get_event_loop()
    lib_type = "asynctnt"
    if args.aiotnt:
        lib_type = "aiotarantool"
    if args.tarantool:
        lib_type = "tarantool"
    print(
        'Running \'{}\' for {} {} operation in {} batches. Using uvloop: {}\n'.format(lib_type, args.n, args.t, args.b,
                                                                                      args.uvloop))

    if args.aiotnt:
        loop.run_until_complete(
            bench_aiotarantool(args.n, args.b, loop=loop, bench_type=args.t)
        )
    elif args.tarantool:
        bench_tarantool(args.n, 1, bench_type=args.t)
    else:
        loop.run_until_complete(
            bench_asynctnt(args.n, args.b, loop=loop, bench_type=args.t)
        )


async def bench_asynctnt(n, b, loop=None, bench_type=TypeBench.SELECT):
    import asynctnt
    from asynctnt.iproto.protocol import Iterator

    loop = loop or asyncio.get_event_loop()

    conn = asynctnt.Connection(host='127.0.0.1',
                               port=3305,
                               username='t1',
                               password='t1',
                               reconnect_timeout=1, loop=loop)
    await conn.connect()

    n_requests_per_bulk = int(math.ceil(n / b))

    async def bulk_f():
        for _ in range(n_requests_per_bulk):
            if bench_type == TypeBench.PING:
                await conn.ping()
            if bench_type == TypeBench.CALL:
                await conn.call('test')
            if bench_type == TypeBench.EVAL:
                await conn.eval('return "hello"')
            if bench_type == TypeBench.SELECT:
                await conn.select(512)
            if bench_type == TypeBench.SQL:
                await conn.sql('select 1 as a, 2 as b')
            if bench_type == TypeBench.REPLACE:
                await conn.replace('tester', [2, 'hhhh'])
            if bench_type == TypeBench.UPDATE:
                await conn.update('tester', [2], [(':', 1, 1, 3, 'yo!')])

    coros = [bulk_f() for _ in range(b)]

    start = datetime.datetime.now()
    await asyncio.wait(coros, loop=loop)
    end = datetime.datetime.now()

    elapsed = end - start
    print('Elapsed: {}, RPS: {}. TPR: {}'.format(elapsed, n / elapsed.total_seconds(), elapsed.total_seconds() / n))


async def bench_aiotarantool(n, b, loop=None, bench_type=TypeBench.SELECT):
    import aiotarantool

    loop = loop or asyncio.get_event_loop()
    conn = aiotarantool.connect('127.0.0.1', 3305,
                                user='t1', password='t1',
                                loop=loop)

    n_requests_per_bulk = int(math.ceil(n / b))

    async def bulk_f():
        for _ in range(n_requests_per_bulk):
            if bench_type == TypeBench.PING:
                await conn.ping()
            if bench_type == TypeBench.CALL:
                await conn.call('test')
            if bench_type == TypeBench.EVAL:
                await conn.eval('return "hello"')
            if bench_type == TypeBench.SELECT:
                await conn.select(512)
            if bench_type == TypeBench.SQL:
                raise TypeError("doesn't have sql eval")
            if bench_type == TypeBench.REPLACE:
                await conn.replace('tester', [2, 'hhhh'])
            if bench_type == TypeBench.UPDATE:
                await conn.update('tester', [2], [(':', 1, 1, 3, 'yo!')])

    coros = [bulk_f() for _ in range(b)]

    start = datetime.datetime.now()
    await asyncio.wait(coros, loop=loop)
    end = datetime.datetime.now()

    elapsed = end - start
    print('Elapsed: {}, RPS: {}'.format(elapsed, n / elapsed.total_seconds()))


def bench_tarantool(n, b, loop=None, bench_type=TypeBench.SELECT):
    import tarantool

    conn = tarantool.Connection(host='127.0.0.1',
                                port=3305,
                                user='t1',
                                password='t1')
    conn.connect()
    b = 1
    n_requests_per_bulk = int(math.ceil(n / b))

    start = datetime.datetime.now()
    for _ in range(n_requests_per_bulk):
        if bench_type == TypeBench.PING:
            conn.ping()
        if bench_type == TypeBench.CALL:
            conn.call('test')
        if bench_type == TypeBench.EVAL:
            conn.eval('return "hello"')
        if bench_type == TypeBench.SELECT:
            conn.select(512)
        if bench_type == TypeBench.SQL:
            raise TypeError("doesn't have sql eval")
        if bench_type == TypeBench.REPLACE:
            conn.replace('tester', (2, 'hhhh'))
        if bench_type == TypeBench.UPDATE:
            conn.update('tester', 2, [(':', 1, 1, 3, 'yo!')])

    end = datetime.datetime.now()
    elapsed = end - start
    print('Elapsed: {}, RPS: {}'.format(elapsed, n / elapsed.total_seconds()))


if __name__ == '__main__':
    main()
