import asyncio
import logging
import sys

import asynctnt

logging.basicConfig(level=logging.DEBUG)


async def main():
    c = asynctnt.Connection(
        host='localhost',
        port=3305,
        connect_timeout=5,
        request_timeout=5,
        reconnect_timeout=1/3,
    )
    async with c:
        while True:
            res = await c.eval('local t ={}; for i=1,1000000 do t[i] = {i + 0.03} end; return t')
            print(sys.getrefcount(res.body[0][-1]))


asyncio.run(main())
