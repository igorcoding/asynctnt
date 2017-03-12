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
    end)

Python code:

.. code:: python

    import asyncio
    import asynctnt


    async def run():
        conn = asynctnt.Connection(host='127.0.0.1', port=3301)
        await conn.connect()

        for i in range(1, 11):
            await conn.insert('tester', [i, 'hello{}'.format(i)])

        values = await conn.select('tester', [])
        print('Code: {}'.format(values.code))
        print('Data: {}'.format(values.body))
        print(values.body2yaml())  # prints as yaml

        await conn.disconnect()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(run())

Stdout:

::

    Code: 0
    Data: [[1, 'hello1'], [2, 'hello2'], [3, 'hello3'], [4, 'hello4']]
    - [1, hello1]
    - [2, hello2]
    - [3, hello3]
    - [4, hello4]

Example of using space format information
-----------------------------------------

Tarantool config:

.. code:: lua

    box.cfg {
        listen = '127.0.0.1:3301'
    }

    box.once('v1', function()
        box.schema.user.grant('guest', 'read,write,execute', 'universe')

        local s = box.schema.create_space('tester')
        s:create_index('primary')
        s:format({  -- <--- Note this format() call
            {name='id', type='unsigned'},
            {name='text', type='string'},
        })
    end)

Python code:

.. code:: python

    import asyncio
    import asynctnt


    async def run():
        conn = asynctnt.Connection(host='127.0.0.1', port=3301,
                                   tuple_as_dict=True)  # <--- Note this flag
        await conn.connect()

        for i in range(1, 5):
            await conn.insert('tester', {  # <--- Note using dict as a tuple
                'id': i,
                'text': 'hello{}'.format(i)
            })

        values = await conn.select('tester', [])
        print('Code: {}'.format(values.code))
        print('Data: {}'.format(values.body))
        print(values.body2yaml())  # prints as yaml

        await conn.disconnect()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(run())

Stdout (now got dict tuples instead of plain arrays):

::

    Code: 0
    Data: [{'id': 1, 'text': 'hello1'}, {'id': 2, 'text': 'hello2'}, {'id': 3, 'text': 'hello3'}, {'id': 4, 'text': 'hello4'}]
    - {id: 1, text: hello1}
    - {id: 2, text: hello2}
    - {id: 3, text: hello3}
    - {id: 4, text: hello4}
