.. _asynctnt-pushes:

Session Push
============

Tarantool 1.10 introduced session pushes which gives an ability to receive
out of bound notifications from Tarantool server

For example let's consider this simple example:

.. code:: lua

    function sub(n)
        for i=1,n do
            box.session.push(i, i * i)
        end

        return 'done'
    end


This function will yield `n` push messages to the client before returning.
To receive such notification in Python using `asynctnt` we need to subscribe
first for these notifications and then use `PushIterator` to iterate over
all the messages from Tarantool:

.. code:: python

    import asyncio
    import asynctnt


    async def main():
        async with asynctnt.Connection(port=3301) as conn:
            fut = conn.call('sub', [10], push_subscribe=True)
            it = asynctnt.PushIterator(fut)

            async for value in it:
                print(value)

    asyncio.run(main())


This will produce the following output:

.. code::

    $ python example.py
    [1, 1]
    [2, 4]
    [3, 9]
    [4, 16]
    [5, 25]


In order to receive a return value you can simply `await` on the initially
returned future from the `call()` method:

.. code:: python

    import asyncio
    import asynctnt


    async def main():
        async with asynctnt.Connection(port=3301) as conn:
            fut = conn.call('sub', [10], push_subscribe=True)
            it = asynctnt.PushIterator(fut)

            async for value in it:
                print(value)

            print(await fut)  # receive the response

    asyncio.run(main())
