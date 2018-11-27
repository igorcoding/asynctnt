import asynctnt
import asyncio


async def f():
    c = await asynctnt.connect()
    c.select(281)


loop = asyncio.get_event_loop()
res = loop.run_until_complete(f())

t = res[0]
print(type(res), len(res))
print(type(t), len(t))

print(t)
print(list(t))
for k, v in t.items():
    print(k , v)
