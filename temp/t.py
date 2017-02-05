# import pyximport; pyximport.install()

import asyncio
# import uvloop; asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
import logging
import os
import sys

from asynctnt import Iterator
from asynctnt.exceptions import TarantoolConnectionLostError
from asynctnt.instance import TarantoolInstance

logging.basicConfig(format='%(created)f [%(module)s:%(funcName)s:%(lineno)d] %(levelname)s: %(message)s',
                    level=logging.DEBUG, stream=sys.stdout)

import datetime

import asynctnt

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))


def read_applua():
    path = os.path.join(CURRENT_DIR, os.pardir, 'tests', 'files', 'app.lua')
    with open(path, 'r') as f:
        return f.read()


async def main(loop):
    tnt = TarantoolInstance(
        applua=read_applua(),
        cleanup=True,
        loop=loop
    )

    # await tnt.start()
    conn = None
    try:
        coro = asyncio.ensure_future(
            asynctnt.connect(host=tnt.host, port=tnt.port,
                             username='t1', password='t1',
                             fetch_schema=True,
                             reconnect_timeout=0.000001, request_timeout=2,
                             encoding='utf-8',
                             loop=loop),
            loop=loop
        )
        await asyncio.sleep(1, loop=loop)
        await tnt.start()
        conn = await coro

        print('connected')
        # print(conn._protocol.schema)
        # print(conn._protocol._con_state)
        #
        # res = await conn.call16('long', [3])
        # res = await conn.auth('tt', 'ttp')d

        await tnt.stop()
        await tnt.start()
        print('RESTARTED TNT')
        # await asyncio.sleep(1, loop=loop)

        # res = await conn.insert('tester', [1, 'привет'])
        # print(res.body2yaml())
        res = await conn.select('_space')
        print(res.body)
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
    except Exception as e:
        logging.info(str(e), exc_info=e)
    finally:
        if conn is not None:
            await conn.disconnect()
        await tnt.stop()


loop = asyncio.get_event_loop()
loop.run_until_complete(main(loop))
