# import pyximport; pyximport.install()

import asyncio
import uvloop;

from asynctnt.iproto.protocol import Iterator

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
import logging
import sys

logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)

import datetime

import asynctnt


async def main():
    conn = asynctnt.Connection(host='127.0.0.1', port=3303, username='tt2', password='ttp2',
                               reconnect_timeout=1)
    await conn.connect()
    
    n_requests = 10000
    
    start = datetime.datetime.now()
    
    coros = []
    
    for _ in range(n_requests):
        # await conn.ping()
        # coros.append(conn.ping())
        
        # await conn.call('test', timeout=1)
        # coros.append(conn.call('test', timeout=1))

        # await conn.eval('return box.info')
        # coros.append(conn.eval('return box.info'))

        await conn.select('tester', iterator=Iterator.LE)
        # coros.append(conn.select(280))

        # await conn.auth('tt2', 'ttp2')
        # await conn.insert('tester', [_])
        # coros.append(conn.replace('tester', [_, 'hello']))

        # await conn.update('tester', [2], [(':', 1, 1, 3, 'yo!')])
        # coros.append(conn.update('tester', [2], [(':', 1, 1, 3, 'yo!')]))

    # start = datetime.datetime.now()
    if coros:
        await asyncio.wait(coros)
    
    end = datetime.datetime.now()
    elapsed = end - start
    print('Elapsed: {}, RPS: {}'.format(elapsed, n_requests / elapsed.total_seconds()))


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
