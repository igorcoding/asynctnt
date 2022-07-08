import asyncio

import asynctnt
import logging

from asynctnt import Metadata, Field

logging.basicConfig(level=logging.DEBUG)


async def main():
    conn = await asynctnt.connect()

    # res = await conn.select('_vspace')
    metadata = Metadata()
    f = Field()
    f.name = 'field1'
    f.type = 'unsigned'
    metadata.add_field(f)
    print(metadata)
    print(metadata.fields)

asyncio.run(main())
