.. _asynctnt-api:

=============
API Reference
=============

.. module:: asynctnt
    :synopsis: A fast Tarantool Database connector for Python/asyncio.

.. currentmodule:: asynctnt


Connection
==========

.. autofunction:: asynctnt.connect


.. autoclass:: asynctnt.Connection
   :members:

   .. automethod:: __init__


PushIterator
============

.. autoclass:: asynctnt.PushIterator
   :members:

   .. automethod:: __init__

Response
========

.. autoclass:: asynctnt.Response
   :members:


Iterator
========

.. autoclass:: asynctnt.Iterator
   :members:


Exceptions
==========

.. autoclass:: asynctnt.exceptions.TarantoolError
   :members:

.. autoclass:: asynctnt.exceptions.TarantoolSchemaError
   :members:

.. autoclass:: asynctnt.exceptions.TarantoolDatabaseError
   :members:

.. autoclass:: asynctnt.exceptions.TarantoolNotConnectedError
   :members:

.. autoclass:: asynctnt.exceptions.ErrorCode
   :members:
