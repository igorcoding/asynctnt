.. _asynctnt-intro:

========
asynctnt
========

.. image:: https://travis-ci.org/igorcoding/asynctnt.svg?branch=master
    :target: https://travis-ci.org/igorcoding/asynctnt

.. image:: https://img.shields.io/pypi/v/asynctnt.svg
    :target: https://pypi.python.org/pypi/asynctnt


asynctnt is a high-performance `Tarantool <https://tarantool.org/>`__
database connector library for Python/asyncio. It was highly inspired by
`asyncpg <https://github.com/MagicStack/asyncpg>`__ module.
asynctnt requires Python 3.5 or later and is supported for Tarantool
versions 1.6+.

Key features
------------
-  Support for all of the basic requests that Tarantool supports. This includes:
   `insert`, `select`, `update`, `upsert`, `eval`, `sql` (for Tarantool 2.x),
   `call` and `call16`.

   *Note: For the difference between `call16` and `call` please refer to
   Tarantool documentation.*
-  **Schema fetching** on connection establishment, so you can use spaces and
   indexes names rather than their ids.
-  Schema **auto refetching**. Tarantool has an option to check if "your" schema
   is up to date, and if not - returns an error. If such an error occurs on any
   request - new schema is refetched and the initial request is resent.
-  **Auto reconnect**. If connection is lost for some reason - asynctnt will
   start automatic reconnection procedure (with authorization and schema
   fetching, of course).
-  Ability to use **dicts for tuples** with field names as keys in DML requests
   (select, insert, replace, delete, update, upsert). This is possible only
   if space.format is specified in Tarantool. Field names can also be used
   in update operations instead of field numbers. Moreover, tuples are decoded
   into the special structures that can act either as `tuple`s or by `dict`s with
   the appropriate API.
-  All requests support specification of `timeout` value, so if request is
   executed for too long, asyncio.TimeoutError is raised. It drastically
   simplifies your code, as you don't need to use `asyncio.wait_for(...)`
   stuff anymore.


License
-------
asynctnt is developed and distributed under the Apache 2.0 license.


.. toctree::
   :maxdepth: 2
   :caption: Contents:

   installation
   examples
   pushes
   api



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
