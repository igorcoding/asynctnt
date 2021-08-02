import asyncio
import asynctnt

import logging

logging.basicConfig(level=logging.DEBUG)


async def main():
    conn = asynctnt.Connection(host='127.0.0.1', port=3301, username="storage", password="passw0rd")
    await conn.connect()
    logging.info('connected')

    await conn.call('box.schema.space.create', ['geo', {"if_not_exists": True}])
    logging.info('space_create')
    await conn.call('box.space.geo:format', [[{"name": "id", "type": "string"}]])
    logging.info('space_format')

    await conn.disconnect()


asyncio.run(main())
