# asynctnt

[![Build](https://github.com/igorcoding/asynctnt/actions/workflows/actions.yaml/badge.svg?branch=master)](https://github.com/igorcoding/asynctnt/actions)
[![PyPI](https://img.shields.io/pypi/v/asynctnt.svg)](https://pypi.python.org/pypi/asynctnt)
[![Maintainability](https://api.codeclimate.com/v1/badges/6cec8adae280cda3e161/maintainability)](https://codeclimate.com/github/igorcoding/asynctnt/maintainability)
<a href="http://tarantool.org">
	<img src="https://avatars2.githubusercontent.com/u/2344919?v=2&s=250" align="right">
</a>

asynctnt is a high-performance [Tarantool](https://tarantool.org/) database
connector library for Python/asyncio. It is highly inspired by
[asyncpg](https://github.com/MagicStack/asyncpg) module.

asynctnt requires Python 3.5 or later and is supported for Tarantool
versions 1.6+.


## Installation
Use pip to install:
```bash
$ pip install asynctnt
```


## Documentation

Documentation is available [here](https://igorcoding.github.io/asynctnt).


## Key features

* Support for all of the basic requests that Tarantool supports. This includes:
  `insert`, `select`, `update`, `upsert`, `eval`, `sql` (for Tarantool 2.x),
  `call` and `call16`. _Note: For the difference between `call16` and `call`
  please refer to Tarantool documentation._
* **Schema fetching** on connection establishment, so you can use spaces and
  indexes names rather than their ids.
* Schema **auto refetching**. If schema in Tarantool is changed, `asynctnt`
  refetches it.
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
  executed for too long, asyncio.TimeoutError is raised. It drastically
  simplifies your code, as you don't need to use `asyncio.wait_for(...)`
  stuff anymore.


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

*(note that you can simultaneously access fields either by indices
or by their names)*
```
tuple: <TarantoolTuple id=1 name='hello1'>
tuple[0]: 1; tuple["id"]: 1
tuple[1]: hello1; tuple["name"]: hello1
```

## SQL

Tarantool 2 brings out an SQL interface to the database. You can easily use SQL
through `asynctnt`

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

    await conn.sql("insert into users (id, name) values (1, 'James Bond')")
    await conn.sql("insert into users (id, name) values (2, 'Ethan Hunt')")
    data = await conn.sql('select * from users')

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

## Performance

On all of the benchmarks below `wal_mode = none`

### Sequential

RPS on running 40k requests (no `uvloop`):

| Request       | aiotarantool  | asynctnt  |
| ------------- |:-------------:| ---------:|
| ping          | 5010.60       | 9037.07   |
| call          | 4575.98       | 9113.32   |
| eval          | 4096.32       | 8921.95   |
| select        | 4063.15       | 9681.12   |
| insert        | 4038.04       | 9332.21   |
| update        | 3945.12       | 10532.75  |


RPS on running 40k requests (with `uvloop`):

| Request       | aiotarantool  | asynctnt  |
| ------------- |:-------------:| ---------:|
| ping          | 7204.31       | 20372.59  |
| call          | 6723.58       | 17279.21  |
| eval          | 7001.27       | 16642.67  |
| select        | 7028.03       | 17730.24  |
| insert        | 7054.06       | 17384.26  |
| update        | 6618.01       | 15990.12  |


### Parallel coroutines

RPS on running 200k requests in 300 parallel coroutines (no `uvloop`):

| Request       | aiotarantool  | asynctnt  |
| ------------- |:-------------:| ---------:|
| ping          | 32946.25      | 44090.53  |
| call          | 29005.93      | 41129.16  |
| eval          | 28792.84      | 44097.02  |
| select        | 26929.76      | 35853.33  |
| insert        | 27142.52      | 31329.85  |
| update        | 25330.98      | 36281.59  |


Let's enable uvloop. This is where asynctnt shines.
RPS on running 200k requests in 300 parallel coroutines (with `uvloop`):


| Request       | aiotarantool  | asynctnt   |
| ------------- |:-------------:| ----------:|
| ping          | 38962.01      | 134043.41  |
| call          | 32799.71      | 99866.28   |
| eval          | 27608.09      | 91056.69   |
| select        | 27436.98      | 108940.41  |
| insert        | 33247.57      | 102971.13  |
| update        | 28544.68      | 98643.46   |


## License
asynctnt is developed and distributed under the Apache 2.0 license.


## References
1. [Tarantool](https://tarantool.org) - in-memory database and application server.
2. [aiotarantool](https://github.com/shveenkov/aiotarantool) - alternative Python/asyncio connector
3. [asynctnt-queue](https://github.com/igorcoding/asynctnt-queue) - bindings on top of `asynctnt` for [tarantool-queue](https://github.com/tarantool/queue)
