# Type Extensions

Tarantool supports natively Decimal, uuid, Datetime and Interval types. `asynctnt` also supports
encoding/decoding of such types to Python native `Decimal`, `UUID` and `datetime` types respectively.

Some examples:

```lua
local s = box.schema.create_space('wallets')
s:format({
    { type = 'unsigned', name = 'id' },
    { type = 'uuid', name = 'uuid' },
    { type = 'decimal', name = 'money' },
    { type = 'datetime', name = 'created_at' },
})
s:create_index('primary')
```

And some python usage:

```python
import pytz
import datetime
import uuid
import asynctnt

from decimal import Decimal

Moscow = pytz.timezone('Europe/Moscow')

conn = await asynctnt.connect()

await conn.insert('wallets', {
    'id': 1,
    'uuid': uuid.uuid4(),
    'money': Decimal('42.17'),
    'created_at': datetime.datetime.now(tz=Moscow)
})
```

## Interval types

Tarantool has support for an interval type. `asynctnt` also has a support for this type which can be used as follows:

```python
import asynctnt

async with asynctnt.Connection() as conn:
    resp = await conn.eval("""
        local datetime = require('datetime')
        return datetime.interval.new({
            year=1,
            month=2,
            week=3,
            day=4,
            hour=5,
            min=6,
            sec=7,
            nsec=8,
        })
    """)

    assert resp[0] == asynctnt.MPInterval(
        year=1,
        month=2,
        week=3,
        day=4,
        hour=5,
        min=6,
        sec=7,
        nsec=8,
    )
```

You may use `asynctnt.MPInterval` type also as parameters to Tarantool methods (like call, insert, and others).
