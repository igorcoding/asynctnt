import pyximport; pyximport.install()

import asyncio
# import uvloop; asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
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
    
    # await conn.auth('tt2', 'ttp2')

    # await conn.disconnect()
    # await conn.connect()
    
    # try:
    #     res = await conn.call('long', 4, timeout=2)
    #     print(res.data)
    # except asyncio.TimeoutError:
    #     print('timeout!')
    #
    n_requests = 10

    try:
        for _ in range(n_requests):
            try:
                res = await conn.ping()
                print(res)
            except Exception as e:
                print(e)
            await asyncio.sleep(1)
    except Exception as e:
        print(e)
    
    print('all')
    # await asyncio.sleep(2)
    
loop = asyncio.get_event_loop()
loop.run_until_complete(main())
