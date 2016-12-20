import asyncio
import datetime

import asynctnt

async def main():
    conn = asynctnt.Connection(host='127.0.0.1', port=3303)
    await conn.connect()

    n_requests = 50000
    start = datetime.datetime.now()

    for _ in range(n_requests):
        await conn.ping()

    end = datetime.datetime.now()
    elapsed = end - start
    print('Elapsed: {}, RPS: {}'.format(elapsed, n_requests / elapsed.total_seconds()))
    
    await asyncio.sleep(2)
    
loop = asyncio.get_event_loop()
loop.run_until_complete(main())
