# Type Extensions

Tarantool supports natively Decimal, uuid and Datetime types. `asynctnt` also supports
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
