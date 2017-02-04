# import pyximport; pyximport.install()

import asyncio
# import uvloop; asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
import logging
import sys

from asynctnt import Iterator
from asynctnt.exceptions import TarantoolConnectionLostError

logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)

import datetime

import asynctnt


async def main():
    conn = await asynctnt.connect(host='127.0.0.1', port=3303,
                                  username='tt2', password='ttp2',
                                  reconnect_timeout=1, request_timeout=2)
    print('connected')
    # print(conn._protocol.schema)
    # print(conn._protocol._con_state)
    #
    # res = await conn.call16('long', [3])
    # res = await conn.auth('tt', 'ttp')d

    # res = await conn.select('tester', [3], iterator=Iterator.GE)
    # print(res.body)
    # res = await conn.eval('return box.cfg')
    # res = await conn.call('test', timeout=0)
    # res = await conn.call('long', [15])
    # res = await conn.refetch_schema()
    # res = await conn.replace('tester', [2, 'hello', 3])
    # res = await conn.update('tester', [2], [(':', 1, 1, 3, 'yo!')])
    # res = await conn.update('tester', [3], [(':', 1, 1, 3, 'yo!')])
    # await conn.upsert('tester', [2, 'hello'], [(':', 2, 1, 3, 'yo!')])
    # await conn.delete('tester', [2], index='primary')
    # print(res.body2yaml())
    await conn.disconnect()

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

    # try:
    #     for _ in range(n_requests):
    #         try:
    #             res = await conn.ping()
    #             print(res)
    #         except Exception as e:
    #             print(e)
    #         await asyncio.sleep(1)
    # except Exception as e:
    #     print(e)

    print('all')
    # await asyncio.sleep(2)


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
