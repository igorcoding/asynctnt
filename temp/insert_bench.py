import asyncio
import uvloop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

import datetime

import asynctnt

cnt = 0

n_requests = 2500 * 40

async def insert_job(tnt):
    global cnt

    for it in range(2500):
        cnt += 1
        r = await tnt.insert(512, [cnt, cnt])


async def main():
    conn = asynctnt.Connection(host='127.0.0.1', port=3303)
    await conn.connect()
    tasks = [asyncio.ensure_future(insert_job(conn)) for _ in range(40)]
    start = datetime.datetime.now()

    await asyncio.wait(tasks)

    end = datetime.datetime.now()
    elapsed = end - start
    print('Elapsed: {}, RPS: {}'.format(elapsed, n_requests / elapsed.total_seconds()))

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()
