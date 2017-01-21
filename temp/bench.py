# import pyximport; pyximport.install()

import asyncio
import uvloop; asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
import logging
import sys

from asynctnt.protocol import ConnectionLostError

logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)

import datetime

import asynctnt


async def main():
    conn = asynctnt.Connection(host='127.0.0.1', port=3303, username='tt2', password='ttp2',
                               reconnect_timeout=1)
    await conn.connect()
    
    n_requests = 1
    
    start = datetime.datetime.now()
    
    coros = []
    
    for _ in range(n_requests):
        # await conn.ping(timeout=0)
        # coros.append(conn.ping())
        res = await conn.call('test')
        # coros.append(conn.call('test'))
    
    if coros:
        await asyncio.wait(coros)
    
    end = datetime.datetime.now()
    elapsed = end - start
    print('Elapsed: {}, RPS: {}'.format(elapsed, n_requests / elapsed.total_seconds()))


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
