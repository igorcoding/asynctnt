# asynctnt

[![Build](https://github.com/igorcoding/asynctnt/actions/workflows/actions.yaml/badge.svg?branch=master)](https://github.com/igorcoding/asynctnt/actions)
[![PyPI](https://img.shields.io/pypi/v/asynctnt.svg)](https://pypi.python.org/pypi/asynctnt)
[![Maintainability](https://api.codeclimate.com/v1/badges/6cec8adae280cda3e161/maintainability)](https://codeclimate.com/github/igorcoding/asynctnt/maintainability)
<a href="http://tarantool.org">
	<img src="https://avatars2.githubusercontent.com/u/2344919?v=2&s=250" align="right">
</a>

asynctnt is a high-performance [Tarantool](https://tarantool.org/) database
connector library for Python/asyncio. It was highly inspired by
[asyncpg](https://github.com/MagicStack/asyncpg) module.

asynctnt requires Python 3.7 or later and is supported for Tarantool
versions 1.10+.


## Installation
Use pip to install:
```bash
$ pip install asynctnt
```


## Documentation

Documentation is available [here](https://igorcoding.github.io/asynctnt).


## Key features

* Support for all the **basic requests** that Tarantool supports. This includes:
  `insert`, `select`, `update`, `upsert`, `call`, `eval`, `execute`.
* Full support for [SQL](https://www.tarantool.io/en/doc/latest/tutorials/sql_tutorial/),
  including [prepared statements](https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_sql/prepare/).
* Support for [interactive transaction](https://www.tarantool.io/en/doc/latest/book/box/atomic/txn_mode_mvcc/) via Tarantool streams.
* Support of `Decimal`, `UUID` and `datetime` types natively.
* Support for parsing [custom errors](https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_error/new/).
* **Schema fetching** on connection establishment, so you can use spaces and
  indexes names rather than their ids, and **auto refetching** if schema in
  Tarantool is changed
* **Auto reconnect**. If connection is lost for some reason - asynctnt will
  start automatic reconnection procedure (with authorization and schema
  fetching, of course).
* Ability to use **dicts for tuples** with field names as keys in DML requests
  (select, insert, replace, delete, update, upsert). This is possible only
  if space.format is specified in Tarantool. Field names can also be used
  in update operations instead of field numbers. Moreover, tuples are decoded
  into the special structures that can act either as `tuple`s or by `dict`s with
  the appropriate API.
* All requests support specification of `timeout` value, so if request is
  executed for too long, asyncio.TimeoutError is raised.


## Basic Usage

Tarantool config:

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
        { name = 'uuid', type = 'uuid' },
    })
end)
```

Python code:

```python
import uuid
import asyncio
import asynctnt


async def main():
    conn = asynctnt.Connection(host='127.0.0.1', port=3301)
    await conn.connect()

    for i in range(1, 11):
        await conn.insert('tester', [i, 'hello{}'.format(i), uuid.uuid4()])

    data = await conn.select('tester', [])
    tup = data[0]
    print('tuple:', tup)
    print(f'{tup[0]=}; {tup["id"]=}')
    print(f'{tup[1]=}; {tup["name"]=}')
    print(f'{tup[2]=}; {tup["uuid"]=}')

    await conn.disconnect()


asyncio.run(main())
```

Stdout:

*(note that you can simultaneously access fields either by indices
or by their names)*
```
tuple: <TarantoolTuple id=1 name='hello1' uuid=UUID('ebbad14c-f78c-42e8-bd12-bfcc564443a6')>
tup[0]=1; tup["id"]=1
tup[1]='hello1'; tup["name"]='hello1'
tup[2]=UUID('ebbad14c-f78c-42e8-bd12-bfcc564443a6'); tup["uuid"]=UUID('ebbad14c-f78c-42e8-bd12-bfcc564443a6')
```

## SQL

Tarantool 2.x brought out an SQL interface to the database. You can easily use it
in `asynctnt`

```lua
box.cfg {
    listen = '127.0.0.1:3301'
}

box.once('v1', function()
    box.schema.user.grant('guest', 'read,write,execute', 'universe')

    box.execute([[
        create table users (
            id int primary key,
            name text
        )
    ]])
end)
```

```python
import asyncio
import asynctnt


async def main():
    conn = asynctnt.Connection(host='127.0.0.1', port=3301)
    await conn.connect()

    await conn.execute("insert into users (id, name) values (?, ?)", [1, 'James Bond'])
    await conn.execute("insert into users (id, name) values (?, ?)", [2, 'Ethan Hunt'])
    data = await conn.execute('select * from users')

    for row in data:
        print(row)

    await conn.disconnect()

asyncio.run(main())
```

Stdout:
```
<TarantoolTuple ID=1 NAME='James Bond'>
<TarantoolTuple ID=2 NAME='Ethan Hunt'>
```

More about SQL features in asynctnt please refer to the [documentation](https://igorcoding.github.io/asynctnt/sql.html)

## Performance

Two performance tests were conducted:
1. `Seq` -- Sequentially calling 40k requests and measuring performance
2. `Parallel` -- Sending 200k in 300 parallel coroutines

On all the benchmarks below `wal_mode = none`.
Turning `uvloop` on has a massive effect on the performance, so it is recommended to use `asynctnt` with it

**Benchmark environment**
* MacBook Pro 2020
* CPU: 2 GHz Quad-Core Intel Core i5
* Memory: 16GB 3733 MHz LPDDR4X

Tarantool:
```lua
box.cfg{wal_mode = 'none'}
```

|           |  Seq (uvloop=off) | Seq (uvloop=on) | Parallel (uvloop=off) | Parallel (uvloop=on) |
|-----------|------------------:|----------------:|----------------------:|---------------------:|
| `ping`    |          12940.93 |        19980.82 |              88341.95 |            215756.24 |
| `call`    |          11586.38 |        18783.56 |              74651.40 |            137557.25 |
| `eval`    |          10631.19 |        17040.57 |              61077.84 |            121542.42 |
| `select`  |           9613.88 |        16718.97 |              61584.07 |            152526.21 |
| `insert`  |          10077.10 |        16989.06 |              65594.82 |            135491.25 |
| `update`  |          10832.16 |        16562.80 |              63003.31 |            121892.28 |
| `execute` |          10431.75 |        16967.85 |              58377.81 |             96891.61 |


## License
asynctnt is developed and distributed under the Apache 2.0 license.


## References
1. [Tarantool](https://tarantool.org) - in-memory database and application server.
2. [aiotarantool](https://github.com/shveenkov/aiotarantool) - alternative Python/asyncio connector
3. [asynctnt-queue](https://github.com/igorcoding/asynctnt-queue) - bindings on top of `asynctnt` for [tarantool-queue](https://github.com/tarantool/queue)
