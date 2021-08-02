## v.1.2.2
**Bugs fixed:**
* Show a diag message rather than Lost connection to Tarantool when disconnected (closes #19)


## v.1.2.1

**Other changes:**
* Updated Cython to 0.29.21
* Building wheels for Python 3.9

## v1.2
**Bugs fixed:**
* Fixed hanging PushIterator when connection to Tarantool is lost (#17).

## v1.1
**New features:**
* Parse autoincrement ids in sql response (#14). Thanks to @oleynikandrey
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
  is available either in space or in response from Tarantool (closes #3).
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
  (fixes #2)
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
* Auto reconnect misbehaved on double on_connection_lost trigger (#11)


## v0.1.13

**Changes:**
* Now `connect()` method call of `Connection` class blocks until connected
  even if another `connect()` is happening in parallel. This resolves issue
  of cancelled coroutines if one tries to connect in parallel coroutines.

**Bugs fixed:**
* Connect hanged indefinitely if asynctnt was accidentally disconnected from
  Tarantool and TCP connection was still alive for a moment while trying to
  reconnect (#8).
* Connect to LOADING Tarantool instance without username/password resulted in
  exception NO_SUCH_SPACE (#10).


## v0.1.12

**Bugs fixed:**
* Fixed compatibility issues with Python 3.7
