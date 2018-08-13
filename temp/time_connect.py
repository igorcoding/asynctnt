import asyncio
import time

import asynctnt
import logging

logging.basicConfig(level=logging.DEBUG)


async def run():
    conn = asynctnt.Connection(host='127.0.0.1', port=3305)
    begin = time.time()
    n = 1000
    for i in range(n):
        await conn.connect()
        await conn.disconnect()

    dt = time.time() - begin
    print('Total: {}'.format(dt))
    print('1 connect+disconnect: {}'.format(dt / n))


async def run2():
    conn = asynctnt.Connection(host='127.0.0.1', port=3305)
    await conn.connect()
    begin = time.time()
    n = 10000
    for i in range(n):
        await conn.eval('return box.info')

    dt = time.time() - begin
    print('Total: {}'.format(dt))
    print('1 ping: {}'.format(dt / n))


async def run3():
    conn = asynctnt.Connection(host='127.0.0.1', port=3301)
    await conn.connect()

    begin = time.time()
    n = 10000
    for i in range(n):
        await conn.select('S')
    dt = time.time() - begin
    print('Total: {}'.format(dt))
    print('1 select: {}'.format(dt / n))


async def run4():
    conn = asynctnt.Connection(host='127.0.0.1', port=3301)
    await conn.connect()

    begin = time.time()
    n = 10000
    for i in range(n):
        resp = await conn.sql('select * from s')
    dt = time.time() - begin
    print('Total: {}'.format(dt))
    print('1 sql: {}'.format(dt / n))

    print(resp.encoding)
    print(resp)
    print(list(resp))

loop = asyncio.get_event_loop()
loop.run_until_complete(run3())
loop.run_until_complete(run4())
