import asyncio
import logging
from typing import Any

import asynctnt

logging.basicConfig(level=logging.DEBUG)


def cb(key: str, data: Any):
    print("event", key, data)


async def main():
    conn = await asynctnt.connect()

    watcher1 = await conn.watch("demokey", cb)

    # await watcher1.watch()
    #
    await asyncio.sleep(600)
    # conn.watch_iproto()

    # async def w():
    #
    #         async for event in watcher:
    #             print(event)
    #
    # await asyncio.gather(w(), w())


asyncio.run(main())
