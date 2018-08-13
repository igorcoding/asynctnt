import asynctnt
import asyncio
import logging
from asynctnt.exceptions import TarantoolNotConnectedError

logging.basicConfig(level=logging.DEBUG)


async def main():
    c = asynctnt.Connection(
        host='localhost',
        port=3305,
        connect_timeout=5,
        request_timeout=5,
        reconnect_timeout=1/3,
    )
    try:
        while True:
            if not c.is_connected:
                print('started connecting')
                await c.connect()  # <------------- Hangs here after the Tarantool instance crashes
                print('connected')
            try:
                input('press any key to segfalt...')
                await c.eval('''require('ffi').cast('char *', 0)[0] = 48''')
            except TarantoolNotConnectedError as e:
                print('EXCEPTION:', e.__class__.__name__, e)
    finally:
        await c.disconnect()
        print('disconnected')


asyncio.run(main())