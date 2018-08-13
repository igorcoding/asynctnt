# import asyncio
#
# import asynctnt
#
#
# async def main():
#     conn = asynctnt.Connection(host='127.0.0.1', port=3301)
#     await conn.connect()
#
#     for i in range(1, 11):
#         await conn.replace('tester', [i, 'hello{}'.format(i)])
#
#     data = await conn.select('tester', [])
#     first_tuple = data[0]
#     print('tuple:', first_tuple)
#     print(f'tuple[0]: {first_tuple[0]}; tuple["id"]: {first_tuple["id"]}')
#     print(f'tuple[1]: {first_tuple[1]}; tuple["name"]: {first_tuple["name"]}')
#
#     await conn.disconnect()
#
#
# asyncio.run(main())


import asyncio
from pprint import pprint

import asynctnt
import logging

# logging.basicConfig(level=logging.DEBUG)

async def main():
    conn = asynctnt.Connection(host='127.0.0.1', port=3301)
    await conn.connect()

    # await conn.sql("""
    #     create table users (
    #         id int primary key,
    #         name text
    #     )
    # """)
    # await conn.sql("insert into users (id, name) values (1, 'James Bond')")
    # await conn.sql("insert into users (id, name) values (2, 'Ethan Hunt')")
    # data = await conn.sql('select * from users')
    #
    # for row in data:
    #     print(row)
    #
    # await conn.disconnect()

    print(await conn.sql("select * from users", parse_metadata=False))


asyncio.run(main())