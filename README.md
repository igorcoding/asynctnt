# asynctnt

asynctnt is a high-performance [Tarantool](https://tarantool.org/) database 
connector library for Python/asyncio. It was highly inspired by 
[asyncpg](https://github.com/MagicStack/asyncpg) module.

asynctnt requires Python 3.5 or later and is supported for Tarantool 
versions 1.6+.

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
* All requests support specification of `timeout` value, so if request is 
  executed for too long, asyncio.TimeoutError is raised. It drastically
  simplifies your code, as you don't need to use `asyncio.wait_for(...)`
  stuff anymore.
  
## Performance

On all of the benchmarks below `wal_mode = none` 

RPS on running 200k requests in 300 parallel coroutines (no `uvloop`):

| Request       | aiotarantool  | asynctnt  |
| ------------- |:-------------:| ---------:|
| ping          | 24961.12      | 28155.32  |
| call          | 21748.06      | 22103.14  |
| eval          | 20497.69      | 21456.38  |
| select        | 19968.26      | 23558.00  |
| insert        | 20604.61      | 22256.69  |
| update        | 18852.46      | 21988.80  |


Let's enable uvloop. This is where asynctnt shines.
RPS on running 200k requests in 300 parallel coroutines (with `uvloop`):


| Request       | aiotarantool  | asynctnt  |
| ------------- |:-------------:| ---------:|
| ping          | 30050.55      | 131317.35 |
| call          | 27995.62      | 92207.33  |
| eval          | 25378.59      | 80539.26  |
| select        | 22346.14      | 88748.47  |
| insert        | 25811.84      | 82526.94  |
| update        | 21914.15      | 80865.00  |

  
## Roadmap

* Add support for field names in all operations
  
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
    
    await conn.disconnect()

loop = asyncio.get_event_loop()
loop.run_until_complete(run())
```

## License
asynctnt is developed and distributed under the Apache 2.0 license.
