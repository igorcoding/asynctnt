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
            bench_aiotarantool(args.n, args.b, loop=loop)
        )
    else:
        loop.run_until_complete(
            bench_asynctnt(args.n, args.b, loop=loop)
        )


async def bench_asynctnt(n, b, loop=None):
    import asynctnt
    from asynctnt.iproto.protocol import Iterator

    loop = loop or asyncio.get_event_loop()

    conn = asynctnt.Connection(host='127.0.0.1',
                               port=3303,
                               username='t1',
                               password='t1',
                               reconnect_timeout=1, loop=loop)
    await conn.connect()

    n_requests_per_bulk = math.ceil(n / b)

    start = datetime.datetime.now()

    async def bulk_f():
        for _ in range(n_requests_per_bulk):
            await getattr(conn, method)(*args)
            await conn.ping()
            # await conn.call('test')
            # await conn.eval('return "hello"')
            # await conn.select(512)
            # await conn.replace('tester', [2, 'hhhh'])
            # await conn.update('tester', [2], [(':', 1, 1, 3, 'yo!')])

    coros = []
    for b in range(b):
        coros.append(asyncio.ensure_future(bulk_f(), loop=loop))

    if coros:
        await asyncio.wait(coros, loop=loop)

    end = datetime.datetime.now()
    elapsed = end - start
    print('Elapsed: {}, RPS: {}'.format(elapsed, n / elapsed.total_seconds()))


async def bench_aiotarantool(n, b, loop=None):
    import aiotarantool

    loop = loop or asyncio.get_event_loop()
    conn = aiotarantool.connect('127.0.0.1', 3303,
                                user='t1', password='t1',
                                loop=loop)

    n_requests_per_bulk = math.ceil(n / b)
    start = datetime.datetime.now()

    async def bulk_f():
        for _ in range(n_requests_per_bulk):
            await conn.ping()
            # await conn.call('test')
            # await conn.eval('return "hello"')
            # await conn.select(512)
            # await conn.replace('tester', [2, 'hhhh'])
            # await conn.update('tester', [2], [(':', 1, 1, 3, 'yo!')])

    coros = []
    for b in range(b):
        coros.append(asyncio.ensure_future(bulk_f(), loop=loop))

    if coros:
        await asyncio.wait(coros, loop=loop)

    end = datetime.datetime.now()
    elapsed = end - start
    print('Elapsed: {}, RPS: {}'.format(elapsed, n / elapsed.total_seconds()))


if __name__ == '__main__':
    main()
