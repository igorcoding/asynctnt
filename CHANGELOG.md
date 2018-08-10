v0.2.0

Changes:
* Improved and simplified connect/reconnect process
* Added ContextManager async with protocol for Connection
* Added `is_fully_connected` property to Connection
* Added disconnect Lock

Bugs Fixed:
* Auto reconnect misbehaved on double on_connection_lost trigger (#11) 

v0.1.13

Changes:
* Now `connect()` method call of `Connection` class blocks until connected
  even if another `connect()` is happening in parallel. This resolves issue
  of cancelled coroutines if one tries to connect in parallel coroutines.

Bugs fixed:
* Connect hanged indefinitely if asynctnt was accidentally disconnected from
  Tarantool and TCP connection was still alive for a moment while trying to
  reconnect (#8).
* Connect to LOADING Tarantool instance without username/password resulted in
  exception NO_SUCH_SPACE (#10).

v0.1.12

Bugs fixed:
* Fixed compatibility issues with Python 3.7