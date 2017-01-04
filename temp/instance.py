import asyncio
# import uvloop; asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
import pyximport; pyximport.install()

import logging

from asynctnt.instance import TarantoolInstance


async def main(t, loop):
    await t.start()
    await t.wait_stopped()
    # await t.stop()


logging.basicConfig(level=logging.DEBUG)
event_loop = asyncio.get_event_loop()
asyncio.set_event_loop(None)
asyncio.get_child_watcher().attach_loop(event_loop)

t = TarantoolInstance(loop=event_loop)
try:
    # event_loop.run_until_complete(main(t, event_loop))
    event_loop.run_until_complete(t.start())
    event_loop.run_until_complete(t.wait_stopped())
except KeyboardInterrupt:
    event_loop.run_until_complete(t.stop())
finally:
    event_loop.close()
