# SQL Support

Tarantool 2.x supports SQL interface to the database. `asynctnt` fully supports it including prepared statements and metadata parsing.

## Basic usage

Tarantool config:

```lua
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
```


Python code:

```python
import asyncio
import asynctnt


async def main():
    conn = asynctnt.Connection(host='127.0.0.1', port=3301)
    await conn.connect()

    await conn.execute("insert into users (name) values (?)", ['James Bond'])
    resp = await conn.execute("insert into users (name) values (:name)", [{':name', 'Ethan Hunt'}])

    # get value of auto incremented primary key
    print(resp.autoincrement_ids)

    data = await conn.execute('select * from users')

    for row in data:
        print(row)

    await conn.disconnect()

asyncio.run(main())
```


Stdout:
```
[2]
<TarantoolTuple ID=1 NAME='James Bond'>
<TarantoolTuple ID=2 NAME='Ethan Hunt'>
```


## Metadata

You can access all the metadata associated with the SQL response, like so:

```python
res = await conn.execute("select * from users")
assert res.metadata.fields[0].name == 'ID'
assert res.metadata.fields[0].type == 'integer'

assert res.metadata.fields[1].name == 'NAME'
assert res.metadata.fields[0].type == 'string'
```

## Prepared statement

You can make use of [prepared statements](https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_sql/prepare/)
in order to execute the same requests more optimally. Simple example:

```python
stmt = conn.prepare('select id, name from users where id = ?')
async with stmt:
    user1 = stmt.execute([1])
    assert user1.name == 'James Bond'

    user2 = stmt.execute([2])
    assert user2.name == 'Ethan Hunt'
```

`prepare()` and `unprepare()` calls are called automatically within a prepared statement context manager.
You may want to control prepare/unprepare yourself:

```python
stmt = conn.prepare('select id, name from users where id = ?')

await stmt.prepare()

user1 = stmt.execute([1])
assert user1.name == 'James Bond'

user2 = stmt.execute([2])
assert user2.name == 'Ethan Hunt'

await stmt.unprepare()
```

Named parameters are also supported:

```python
stmt = conn.prepare('select id, name from users where id = :id')
async with stmt:
    user1 = stmt.execute([
        {':id': 1}
    ])
    assert user1.name == 'James Bond'

    user2 = stmt.execute([
        {':id': 2}
    ])
    assert user2.name == 'Ethan Hunt'
```
