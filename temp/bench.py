import argparse
import asyncio
import datetime
import logging
import math
import sys


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
    args = parser.parse_args()

    if args.uvloop:
        import uvloop
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

    loop = asyncio.get_event_loop()

    print('Running \'{}\' for {} requests in {} batches. '
          'Using uvloop: {}\n'.format(
        'aiotarantool' if args.aiotnt else 'asynctnt',
        args.n, args.b, args.uvloop)
    )

    if args.aiotnt:
        loop.run_until_complete(
            bench_aiotarantool(args.n, args.b)
        )
    elif args.tarantool:
        bench_tarantool(args.n, 1)
    else:
        loop.run_until_complete(
            bench_asynctnt(args.n, args.b)
        )


async def bench_asynctnt(n, b):
    import asynctnt
    from asynctnt.iproto.protocol import Iterator

    conn = asynctnt.Connection(host='127.0.0.1',
                               port=3305,
                               username='t1',
                               password='t1',
                               reconnect_timeout=1)
    await conn.connect()

    n_requests_per_bulk = int(math.ceil(n / b))

    async def bulk_f():
        for _ in range(n_requests_per_bulk):
            # await conn.ping()
            # await conn.call('test')
            # await conn.eval('return "hello"')
            await conn.select(512)
            # await conn.sql('select 1 as a, 2 as b')
            # await conn.replace('tester', [2, 'hhhh'])
            # await conn.update('tester', [2], [(':', 1, 1, 3, 'yo!')])

    coros = [bulk_f() for _ in range(b)]

    start = datetime.datetime.now()
    await asyncio.wait(coros)
    end = datetime.datetime.now()

    elapsed = end - start
    print('Elapsed: {}, RPS: {}. TPR: {}'.format(elapsed, n / elapsed.total_seconds(), elapsed.total_seconds() / n))


async def bench_aiotarantool(n, b):
    import aiotarantool

    conn = aiotarantool.connect('127.0.0.1', 3305,
                                user='t1', password='t1')

    n_requests_per_bulk = int(math.ceil(n / b))

    async def bulk_f():
        for _ in range(n_requests_per_bulk):
            await conn.ping()
            # await conn.call('test')
            # await conn.eval('return "hello"')
            # await conn.select(512)
            # await conn.replace('tester', [2, 'hhhh'])
            # await conn.update('tester', [2], [(':', 1, 1, 3, 'yo!')])

    coros = [bulk_f() for _ in range(b)]

    start = datetime.datetime.now()
    await asyncio.wait(coros)
    end = datetime.datetime.now()

    elapsed = end - start
    print('Elapsed: {}, RPS: {}'.format(elapsed, n / elapsed.total_seconds()))


def bench_tarantool(n, b):
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
        # conn.ping()
        # await conn.call('test')
        # await conn.eval('return "hello"')
        conn.select(512)
        # await conn.replace('tester', [2, 'hhhh'])
        # await conn.update('tester', [2], [(':', 1, 1, 3, 'yo!')])

    end = datetime.datetime.now()
    elapsed = end - start
    print('Elapsed: {}, RPS: {}'.format(elapsed, n / elapsed.total_seconds()))


if __name__ == '__main__':
    main()
