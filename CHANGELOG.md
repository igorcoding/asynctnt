# Changelog

## v2.3.0
**New features:**
* Added support for [interval types](https://www.tarantool.io/en/doc/latest/reference/reference_lua/datetime/interval_object/) [#30](https://github.com/igorcoding/asynctnt/issues/30)
* Added ability to retrieve IProto features [available](https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_iproto/feature/) in Tarantool using `conn.features` property


## v2.2.0
**New features:**
* Implemented ability to send update/upsert requests with field names when schema is disabled (`fetch_schema=False`) and when fields are not found in the schema (good example of this case is using json path like `data.inner1.inner2.key1` as a key)

**Bug fixes:**
* Fixed issue with not being able to send Decimals in update statements. Now there are no extra checks - any payload is sent directly to Tarantool (fixes [#34](https://github.com/igorcoding/asynctnt/issues/34))

**Other changes**
* Fixed tests failing on modern Tarantool in the SQL queries.
* Removed from ci/cd testing on macOS python 3.7
* Added Tarantool 3 to CI Testing

## v2.1.0
**Breaking changes:**
* Dropped support for Python 3.6

**New features:**
* Added building wheels for Python 3.11 and support for 3.12
* Added support for PyPy 3.10. It's compiling and working, but there is an obvious performance downgrade compared to CPython.
* Now repr() of TarantoolTuple objects is being truncated to 50 fields

**Bug fixes:**
* Fixed an issue with encoding of update operations as tuples on PyPy

**Other changes**
* Upgraded to Cython 3.0.7
* Using pyproject.toml for building spec
* Using black, isort & ruff for linting
* _testbase.py was moved to tests/_testbase.py

## v2.0.1
* Fixed an issue with encoding datetimes less than 01-01-1970 (fixes [#29](https://github.com/igorcoding/asynctnt/issues/29))
* Fixed "Edit on Github" links in docs (fixes [#26](https://github.com/igorcoding/asynctnt/issues/26))

## v2.0.0
**Breaking changes:**
* `Connection.sql()` method is renamed to `Connection.execute()`
* Drop support for `loop` argument in the `Connection` (fixes [#18](https://github.com/igorcoding/asynctnt/issues/18))

**New features:**
* Added support for `Decimal`, `UUID` and `datetime` types natively using MessagePack extensions
* Added support for SQL prepared statements with `Connection.prepare()` method and
  `PreparedStatement` class
* Added support for interactive transactions and streams (fixes [#21](https://github.com/igorcoding/asynctnt/issues/21))
* Added support for MP_ERROR extensions
* Bind metadata of parameters is available now in the `response.params` and `response.params_count` fields
* Exposed an internal schema as a `Connection.schema` property, introducing new classes to operate the schema with
* Exposed SQL metadata of responses as `response.metadata` field
* Added typings to internal types such as `Connection`, `Response`, `Metadata`, `Schema`, `TarantoolTuple` and others
* `asynctnt` now sends IPROTO_ID request before anything else to notify Tarantool of used features

**Other changes:**
* Updated Cython to 0.29.30
* Update msgpuck to revision 0c6680a300e31714f475a7f90c2d95a02d001d80
* Internal refactoring of requests payload encoding
* Refactoring of schema parsing and unifying under `metadata` name and structure

## v1.2.3
* Support Python 3.10

## v1.2.2
**Bugs fixed:**
* Show a diag message rather than Lost connection to Tarantool when disconnected (closes [#19](https://github.com/igorcoding/asynctnt/issues/19))


## v1.2.1

**Other changes:**
* Updated Cython to 0.29.21
* Building wheels for Python 3.9

## v1.2
**Bugs fixed:**
* Fixed hanging PushIterator when connection to Tarantool is lost ([#17](https://github.com/igorcoding/asynctnt/issues/17)).

## v1.1
**New features:**
* Parse autoincrement ids in sql response ([#14](https://github.com/igorcoding/asynctnt/issues/14)). Thanks to @oleynikandrey
* Added Python 3.8 support. Removed all redundant `loop` arguments to functions
  and asyncio classes.

**Other changes:**
* Updated Cython to 0.29.14
* Updated msgpuck to most recent version.
* Added building wheels for Windows for Python 3.6, 3.7, 3.8

## v1.0
**Breaking changes:**
* Removed method `body2yaml` from Response.
* Option `tuple_as_dict` is removed from `Connection` and all the methods.

**New features:**
* Making Response objects contain TarantoolTuple objects if format information
  is available either in space or in response from Tarantool (closes [#3](https://github.com/igorcoding/asynctnt/issues/3)).
* TarantoolTuple objects are index-agnostic, meaning one can access tuple value
  either by numeric index or by a key from `space:format()` specification.
* You can directly access Response using indices
  (`resp[0]` instead of `resp.body[0]`).
* Added supported for receiving `box.session.push()` messages from Tarantool
  by introducing new parameter `push_subscribe` to api methods in `Connection`
  and the PushIterator class to iterate over the push messages of a specific
  request.
* Added `Connection.sql` method to execute SQL statements for Tarantool 2
  (see asynctnt docs for details).
* Added internal background coroutine with pings periodically a Tarantool
  instance to check if it is alive and to refresh schema if it is changed
  (default period is 5 seconds and is configured by `Connection.ping_timeout`
  parameter).

**Changes:**
* Iteration over TarantoolTuple results in iterating over a raw tuple by
  indices.
* TarantoolTuple has `.keys()`, `.values()` and `.items()` from the dict
  protocol. All these methods return iterators making it possible to iterate
  over keys, values or key-value pairs accordingly. `keys` and `items` methods
  ignore any extra fields if space format contains less fields than there are
  in the tuple. One can acces those extra fields by index numbers.
* `Connection`'s default `connect_timeout` changed from `60` to `3` seconds.
* `select`: changed default iterator type to `ALL` if no key provided
  (fixes [#2](https://github.com/igorcoding/asynctnt/issues/2))
* `Response` new function `done()` indicates if Response is
  actually finished.
* `schema_id` is not being sent to Tarantool to check against current schema
  version. Instead schema is only checked and refetched if needed only _after_
  the request. This ensures that request is executed in a "constant" time
  rather than unpredicted with possible schema changes.
* Improved `Connection.refetch_schema()` method to ensure there is only one
  currently running refetch process.

**Other changes:**
* `asynctnt` now ships with precompiled wheel packages.
* Changed version numbering.
* Updated `Cython` to version `0.29`
* Updated `msgpuck` version
* Improved speed of asynctnt method calls a bit more


## v0.2.0

**Changes:**
* Improved and simplified connect/reconnect process
* Added ContextManager async with protocol for Connection
* Added `is_fully_connected` property to Connection
* Added disconnect Lock

**Bugs Fixed:**
* Auto reconnect misbehaved on double on_connection_lost trigger ([#11](https://github.com/igorcoding/asynctnt/issues/11))


## v0.1.13

**Changes:**
* Now `connect()` method call of `Connection` class blocks until connected
  even if another `connect()` is happening in parallel. This resolves issue
  of cancelled coroutines if one tries to connect in parallel coroutines.

**Bugs fixed:**
* Connect hanged indefinitely if asynctnt was accidentally disconnected from
  Tarantool and TCP connection was still alive for a moment while trying to
  reconnect ([#8](https://github.com/igorcoding/asynctnt/issues/8)).
* Connect to LOADING Tarantool instance without username/password resulted in
  exception NO_SUCH_SPACE ([#10](https://github.com/igorcoding/asynctnt/issues/10)).


## v0.1.12

**Bugs fixed:**
* Fixed compatibility issues with Python 3.7
