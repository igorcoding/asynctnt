import asyncio
import time

import asynctnt
import logging

logging.basicConfig(level=logging.DEBUG)


async def run():
    conn = asynctnt.Connection(host='127.0.0.1', port=3305)
    await conn.connect()

    fut = conn.call("asyncaction", push_subscribe=True)
    async for row in asynctnt.PushIterator(fut):
        print('wow', row)

    print(await fut)
    # await asyncio.sleep(20)

loop = asyncio.get_event_loop()
loop.run_until_complete(run())
