# asynctnt

[![Build Status](https://travis-ci.org/igorcoding/asynctnt.svg?branch=master)](https://travis-ci.org/igorcoding/asynctnt)
[![PyPI](https://img.shields.io/pypi/v/asynctnt.svg)](https://pypi.python.org/pypi/asynctnt)
[![Maintainability](https://api.codeclimate.com/v1/badges/6cec8adae280cda3e161/maintainability)](https://codeclimate.com/github/igorcoding/asynctnt/maintainability)
<a href="http://tarantool.org">
	<img src="https://avatars2.githubusercontent.com/u/2344919?v=2&s=250" align="right">
</a>

asynctnt is a high-performance [Tarantool](https://tarantool.org/) database 
connector library for Python/asyncio. It was highly inspired by 
[asyncpg](https://github.com/MagicStack/asyncpg) module.

asynctnt requires Python 3.5 or later and is supported for Tarantool 
versions 1.6+.

## Documentation

Documentation is available [here](https://igorcoding.github.io/asynctnt).

## Key features

* Support for all of the basic requests that Tarantool supports. This includes:
  `insert`, `select`, `update`, `upsert`, `eval`, `call` and `call16`. 
  `call16` is an old call method of Tarantool 1.6. `call` - simplifies return
  values of Tarantool procedures (please refer to Tarantool documentation 
  for more details).
* **Schema fetching** on connection establishment, so you can use spaces and 
  indexes names rather than their ids.
* Schema **auto refetching**. Tarantool has an option to check if "your" schema 
  is up to date, and if not - returns an error. If such an error occurs on any 
  request - new schema is refetched and the initial request is resent.
* **Auto reconnect**. If connection is lost for some reason - asynctnt will 
  start automatic reconnection procedure (with authorization and schema 
  fetching, of course).
* Ability to use **dicts for tuples** with field names as keys in DML requests 
  (select, insert, replace, delete, update, upsert). This is possible only 
  if space.format is specified in Tarantool. Field names can also be used 
  in update operations instead of field numbers. Moreover, tuples can be 
  decoded into dicts instead of arrays if `tuple_as_dict` is True either in
  `Connection` or a specific request. See below for examples.
* All requests support specification of `timeout` value, so if request is 
  executed for too long, asyncio.TimeoutError is raised. It drastically
  simplifies your code, as you don't need to use `asyncio.wait_for(...)`
  stuff anymore.
  
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

  
## Installation
Use pip to install:
```bash
$ pip install asynctnt
```


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
end)
```

Python code:
```python
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
```

Stdout:
```
Code: 0
Data: [[1, 'hello1'], [2, 'hello2'], [3, 'hello3'], [4, 'hello4']]
- [1, hello1]
- [2, hello2]
- [3, hello3]
- [4, hello4]
```


## Example of using space format information

Tarantool config:

```lua
box.cfg {
    listen = '127.0.0.1:3301'
}

box.once('v1', function()
    box.schema.user.grant('guest', 'read,write,execute', 'universe')

    local s = box.schema.create_space('tester')
    s:create_index('primary')
    s:format({  --   <--- Note this format() call
        {name='id', type='unsigned'},
        {name='text', type='string'},
    })
end)
```


Python code:
```python
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
```

Stdout (now got dict tuples instead of plain arrays):
```
Code: 0
Data: [{'id': 1, 'text': 'hello1'}, {'id': 2, 'text': 'hello2'}, {'id': 3, 'text': 'hello3'}, {'id': 4, 'text': 'hello4'}]
- {id: 1, text: hello1}
- {id: 2, text: hello2}
- {id: 3, text: hello3}
- {id: 4, text: hello4}
```

## License
asynctnt is developed and distributed under the Apache 2.0 license.


## References
1. [Tarantool](https://tarantool.org) - in-memory database and application server.
2. [aiotarantool](https://github.com/shveenkov/aiotarantool) - alternative Python/asyncio connector
3. [asynctnt-queue](https://github.com/igorcoding/asynctnt-queue) - bindings on top of `asynctnt` for [tarantool-queue](https://github.com/tarantool/queue)
