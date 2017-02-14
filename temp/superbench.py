import argparse
import asyncio
import datetime
import logging
import math
import sys


def main():
    logging.basicConfig(level=logging.WARNING, stream=sys.stdout)
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', type=int, default=200000,
                        help='number of executed requests')
    parser.add_argument('-b', type=int, default=200, help='number of bulks')
    args = parser.parse_args()

    print('Running {} requests in {} batches. '.format(args.n, args.b))

    scenarios = [
        ['ping', []],
        ['call', ['test']],
        ['eval', ['return "hello"']],
        ['select', [512]],
        ['replace', ['tester', [2, 'hhhh']]],
        ['update', ['tester', [2], [(':', 1, 1, 3, 'yo!')]]],
    ]

    for use_uvloop in [False, True]:
        if use_uvloop:
            import uvloop
            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        asyncio.set_event_loop(None)
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        print('--------- uvloop: {} --------- '.format(use_uvloop))

        for bench in [bench_aiotarantool, bench_asynctnt]:
            for scenario in scenarios:
                loop.run_until_complete(
                    bench(args.n, args.b,
                          scenario[0], scenario[1],
                          loop=loop)
                )


async def bench_asynctnt(n, b, method, args=[], loop=None):
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

    coros = []
    for b in range(b):
        coros.append(asyncio.ensure_future(bulk_f(), loop=loop))

    if coros:
        await asyncio.wait(coros, loop=loop)

    end = datetime.datetime.now()
    elapsed = end - start
    print('asynctnt [{}] Elapsed: {}, RPS: {}'.format(
        method, elapsed, n / elapsed.total_seconds()))


async def bench_aiotarantool(n, b, method, args=[], loop=None):
    import aiotarantool

    loop = loop or asyncio.get_event_loop()
    conn = aiotarantool.connect('127.0.0.1', 3303,
                                user='t1', password='t1',
                                loop=loop)

    n_requests_per_bulk = math.ceil(n / b)
    start = datetime.datetime.now()

    async def bulk_f():
        for _ in range(n_requests_per_bulk):
            await getattr(conn, method)(*args)

    coros = []
    for b in range(b):
        coros.append(asyncio.ensure_future(bulk_f(), loop=loop))

    if coros:
        await asyncio.wait(coros, loop=loop)

    end = datetime.datetime.now()
    elapsed = end - start
    print('aiotarantool [{}] Elapsed: {}, RPS: {}'.format(
        method, elapsed, n / elapsed.total_seconds()))


if __name__ == '__main__':
    main()
