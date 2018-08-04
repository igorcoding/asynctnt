v0.1.13

Changes:
* Now `connect()` method call of `Connection` class blocks until connected
  even if another `connect()` is happening in parallel. This resolves issue
  of cancelled coroutines if one tries to connect in parallel coroutines.

Bugs fixed:
* Connect hanged indefinetly if asynctnt was accidentally disconnected from
  Tarantool and TCP connection was still alive for a moment while trying to
  reconnect.


v0.1.12

Bugs fixed:
* Fixed compatibility issues with Python 3.7