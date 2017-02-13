import asyncio
import uvloop; asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
import datetime


def create_tuple(i, size):
    s = 'x' * size
    return []

async def run_asynctnt():
    import asynctnt
    conn = asynctnt.Connection(host='127.0.0.1', port=3303)
    await conn.connect()

    n = 100000
    size = 1.5 * 1024

    start = datetime.datetime.now()

    for i in range(1, n + 1):
        # await conn.insert('tester', [i, 'x' * int(size)])
        # await conn.select('tester', [i])
        await conn.delete('tester', [i])

    # values = await conn.select('tester', [])

    end = datetime.datetime.now()
    elapsed = end - start
    print('Elapsed: {}, RPS: {}'.format(elapsed, n / elapsed.total_seconds()))

    await conn.disconnect()


loop = asyncio.get_event_loop()
loop.run_until_complete(run_asynctnt())
