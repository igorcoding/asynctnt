import argparse
import asyncio
import datetime
import logging
import math
import sys

HOST = "127.0.0.1"
PORT = 3305
USERNAME = "t1"
PASSWORD = "t1"


def main():
    logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-n", type=int, default=200000, help="number of executed requests"
    )
    parser.add_argument("-b", type=int, default=300, help="number of bulks")
    args = parser.parse_args()

    print("Running {} requests in {} batches. ".format(args.n, args.b))

    scenarios = [
        ["ping", []],
        ["call", ["test"]],
        ["call", ["test"], {"push_subscribe": True}],
        ["eval", ['return "hello"']],
        ["select", [512]],
        ["replace", [512, [2, "hhhh"]]],
        ["update", [512, [2], [(":", 1, 1, 3, "yo!")]]],
        ["execute", ["select 1 as a, 2 as b"], {"parse_metadata": False}],
    ]

    for use_uvloop in [True]:
        if use_uvloop:
            try:
                import uvloop
            except ImportError:
                print("No uvloop installed. Skipping.")
                continue

            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        else:
            asyncio.set_event_loop_policy(None)
        asyncio.set_event_loop(None)
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        print("--------- uvloop: {} --------- ".format(use_uvloop))

        for name, conn_creator in [
            ("asynctnt", create_asynctnt),
            # ('aiotarantool', create_aiotarantool),
        ]:
            conn = loop.run_until_complete(conn_creator())
            for scenario in scenarios:
                loop.run_until_complete(
                    async_bench(
                        name,
                        conn,
                        args.n,
                        args.b,
                        method=scenario[0],
                        args=scenario[1],
                        kwargs=scenario[2] if len(scenario) > 2 else {},
                    )
                )


async def async_bench(name, conn, n, b, method, args=None, kwargs=None):
    if kwargs is None:
        kwargs = {}
    if args is None:
        args = []
    n_requests_per_bulk = math.ceil(n / b)

    async def bulk_f():
        for _ in range(n_requests_per_bulk):
            await getattr(conn, method)(*args, **kwargs)

    start = datetime.datetime.now()
    coros = [asyncio.create_task(bulk_f()) for _ in range(b)]

    await asyncio.wait(coros)
    end = datetime.datetime.now()

    elapsed = end - start
    print(
        "{} [{}] Elapsed: {}, RPS: {}".format(
            name, method, elapsed, n / elapsed.total_seconds()
        )
    )


async def create_asynctnt():
    import asynctnt

    conn = asynctnt.Connection(
        host=HOST,
        port=PORT,
        username=USERNAME,
        password=PASSWORD,
        reconnect_timeout=1,
        fetch_schema=False,
        auto_refetch_schema=False,
    )
    await conn.connect()
    return conn


async def create_aiotarantool():
    import aiotarantool

    conn = aiotarantool.connect(HOST, PORT, user=USERNAME, password=PASSWORD)
    await conn.connect()
    return conn


if __name__ == "__main__":
    main()
