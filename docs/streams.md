# Streams and Transactions

## Basic usage
Interactive transactions are available in Tarantool 2.10+ and are implemented on
top of streams.

You can easily use streams in `asynctnt`:

```python
import asynctnt

conn = await asynctnt.connect()

async with conn.stream() as s:
    data = [1, 'Peter Parker']
    await s.insert('heroes', data)
    await s.update('heroes', [1], ['=', 'name', 'Spider-Man'])

res = await conn.select('heroes')
print(res)
```

This syntax will call `begin()` and `commit()` methods behind the scenes and a `rollback()`
method if any exception will happen inside the context manager.

## Isolation
Everything happening inside in the transaction (a.k.a. stream) is visible only
to the current stream.
You may also control the isolation level, but you have to call `begin()` method manually:

```python
import asynctnt
from asynctnt.api import Isolation

conn = await asynctnt.connect()

s = conn.stream()
await s.begin(Isolation.READ_COMMITTED)

data = [1, 'Peter Parker']
await s.insert('heroes', data)
await s.update('heroes', [1], ['=', 'name', 'Spider-Man'])

await s.commit()

res = await conn.select('heroes')
print(res)
```

## Flexibility
Tarantool allows to start/end transaction with any way (of course the native functions are the fastest):
```python
# begin() variants
await conn.begin()
await conn.call('box.begin')
await conn.execute('START TRANSACTION')

# commit() variants
await conn.commit()
await conn.call('box.commit')
await conn.execute('COMMIT')

# rollback() variants
await conn.rollback()
await conn.call('box.rollback')
await conn.execute('ROLLBACK')
```
