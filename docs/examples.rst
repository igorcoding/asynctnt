.. _asynctnt-examples:

Examples
========
Basic Usage
-----------

Tarantool config:

.. code:: lua

    box.cfg {
        listen = '127.0.0.1:3301'
    }

    box.once('v1', function()
        box.schema.user.grant('guest', 'read,write,execute', 'universe')

        local s = box.schema.create_space('tester')
        s:create_index('primary')
        s:format({
            { name = 'id', type = 'unsigned' },
            { name = 'name', type = 'string' },
        })
    end)

Python code:

.. code:: python

    import asyncio
    import asynctnt


    async def main():
        conn = asynctnt.Connection(host='127.0.0.1', port=3301)
        await conn.connect()

        for i in range(1, 11):
            await conn.insert('tester', [i, 'hello{}'.format(i)])

        data = await conn.select('tester', [])
        first_tuple = data[0]
        print('tuple:', first_tuple)
        print(f'tuple[0]: {first_tuple[0]}; tuple["id"]: {first_tuple["id"]}')
        print(f'tuple[1]: {first_tuple[1]}; tuple["name"]: {first_tuple["name"]}')

        await conn.disconnect()

    asyncio.run(main())

Stdout:

::

    tuple: <TarantoolTuple id=1 name='hello1'>
    tuple[0]: 1; tuple["id"]: 1
    tuple[1]: hello1; tuple["name"]: hello1


Using SQL
---------

Tarantool config:

.. code:: lua

    box.cfg {
        listen = '127.0.0.1:3301'
    }

    box.once('v1', function()
        box.schema.user.grant('guest', 'read,write,execute', 'universe')

        box.execute([[
            create table users (
                id int primary key autoincrement,
                name text
            )
        ]])
    end)


Python code:

.. code:: python

    import asyncio
    import asynctnt


    async def main():
        conn = asynctnt.Connection(host='127.0.0.1', port=3301)
        await conn.connect()

        await conn.sql("insert into users (name) values ('James Bond')")
        resp = await conn.sql("insert into users (name) values ('Ethan Hunt')")

        # get value of auto incremented primary key
        print(resp.autoincrement_ids)

        data = await conn.sql('select * from users')

        for row in data:
            print(row)

        await conn.disconnect()

    asyncio.run(main())


Stdout:

.. code::

    <TarantoolTuple ID=1 NAME='James Bond'>
    <TarantoolTuple ID=2 NAME='Ethan Hunt'>


Using Connection context manager
--------------------------------

.. code:: python

    import asyncio
    import asynctnt


    async def main():
        async with asynctnt.Connection(port=3301) as conn:
            res = await conn.call('box.info')
            print(res.body)

    asyncio.run(main())
