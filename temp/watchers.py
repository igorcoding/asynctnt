import asyncio
import logging

import asynctnt

logging.basicConfig(level=logging.DEBUG)


async def main():
    conn = await asynctnt.connect()

    async def w():
        async with conn.watch("demokey") as watcher:
            async for event in watcher:
                print(event)

    await asyncio.gather(w(), w())


asyncio.run(main())
