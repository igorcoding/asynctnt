# Examples

## Basic Usage

```lua
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
```

Python code:

```python
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
```

Stdout:
```
tuple: <TarantoolTuple id=1 name='hello1'>
tuple[0]: 1; tuple["id"]: 1
tuple[1]: hello1; tuple["name"]: hello1
```

## Using Connection context manager

```python
import asyncio
import asynctnt


async def main():
    async with asynctnt.Connection(port=3301) as conn:
        res = await conn.call('box.info')
        print(res.body)

asyncio.run(main())
```

## Connect with SSL encryption
```python
import asyncio
import asynctnt


async def main():
    conn = asynctnt.Connection(host='127.0.0.1',
                               port=3301,
                               transport=asynctnt.Transport.SSL,
                               ssl_key_file='./ssl/host.key',
                               ssl_cert_file='./ssl/host.crt',
                               ssl_ca_file='./ssl/ca.crt',
                               ssl_ciphers='ECDHE-RSA-AES256-GCM-SHA384')
    await conn.connect()

    resp = await conn.ping()
    print(resp)

    await conn.disconnect()

asyncio.run(main())
```

Stdout:
```
<Response sync=4 rowcount=0 data=None>
```
