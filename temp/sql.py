import asyncio
import time

import asynctnt
import logging

logging.basicConfig(level=logging.DEBUG)


async def run():
    conn = asynctnt.Connection(host='127.0.0.1', port=3305)
    await conn.connect()

    resp = await conn.sql("insert into s (id, name) values (1, 'one')")
    # resp = await conn.sql("select * from s")
    print(resp)
    print(resp.rowcount)

loop = asyncio.get_event_loop()
loop.run_until_complete(run())
