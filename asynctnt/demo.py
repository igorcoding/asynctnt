import asyncio


async def super_func(a, b, loop=None):
    await asyncio.sleep(1, loop=loop)
    return a + b
