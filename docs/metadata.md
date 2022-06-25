# Metadata

`asynctnt` fetches schema from Tarantool server in order to provide ability to use
space and index names in the CRUD requests, as well as field names of the tuples.
You can access this metadata in the `Connection` object directly. This schema may be
refreshed if schema is changed in Tarantool.

```python
import asynctnt

conn = await asynctnt.connect()

print('space id', conn.schema.spaces['_space'].sid)
print('space engine', conn.schema.spaces['_space'].engine)
print('space format fields', conn.schema.spaces['_space'].metadata.fields)
```
