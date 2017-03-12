import asyncio
import asynctnt


async def run():
    conn = asynctnt.Connection(host='127.0.0.1', port=3301, tuple_as_dict=True)
    await conn.connect()

    for i in range(1, 5):
        res = await conn.insert('tester', {
            'id': i,
            'text': 'hello{}'.format(i)
        }, tuple_as_dict=True)
        print(res.body)

    values = await conn.select('tester', [])
    print(values)
    print('Code: {}'.format(values.code))
    print('Data: {}'.format(values.body))
    print(values.body2yaml())  # prints as yaml

    await conn.disconnect()

loop = asyncio.get_event_loop()
loop.run_until_complete(run())
