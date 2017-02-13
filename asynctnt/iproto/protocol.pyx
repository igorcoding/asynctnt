cimport cython
cimport cpython.dict

import asyncio
import enum

from asynctnt.exceptions import \
    TarantoolSchemaError, TarantoolNotConnectedError

include "const.pxi"

include "unicode.pyx"
include "buffer.pyx"
include "rbuffer.pyx"
include "request.pyx"
include "response.pyx"
include "schema.pyx"
include "db.pyx"

include "coreproto.pyx"


class Iterator(enum.IntEnum):
    EQ = 0
    REQ = 1
    ALL = 2
    LT = 3
    LE = 4
    GE = 5
    GT = 6
    BITS_ALL_SET = 7
    BITS_ANY_SET = 8
    BITS_ALL_NOT_SET = 9
    OVERLAPS = 10
    NEIGHBOR = 11


cdef class BaseProtocol(CoreProtocol):
    def __init__(self, host, port,
                 username, password,
                 fetch_schema,
                 auto_refetch_schema,
                 connected_fut,
                 on_connection_made, on_connection_lost,
                 loop,
                 request_timeout=None,
                 encoding=None,
                 initial_read_buffer_size=None):
        CoreProtocol.__init__(self, host, port, encoding,
                              initial_read_buffer_size)

        self.loop = loop

        self.username = username
        self.password = password
        self.fetch_schema = fetch_schema
        self.auto_refetch_schema = auto_refetch_schema
        self.request_timeout = request_timeout or 0
        self.connected_fut = connected_fut
        self.on_connection_made_cb = on_connection_made
        self.on_connection_lost_cb = on_connection_lost

        self._on_request_completed_cb = self._on_request_completed
        self._on_request_timeout_cb = self._on_request_timeout

        self._sync = 0
        self._schema = None
        self._schema_id = -1
        self._db = self._create_db()

        try:
            self.create_future = self.loop.create_future
        except AttributeError:
            self.create_future = self._create_future_fallback

    def _create_future_fallback(self):  # pragma: no cover
        return asyncio.Future(loop=self.loop)

    @property
    def schema(self):
        return self._schema

    @property
    def schema_id(self):
        return self._schema_id

    cdef void _set_connection_ready(self):
        if not self.connected_fut.done():
            self.connected_fut.set_result(True)
            self.con_state = CONNECTION_FULL

    cdef void _set_connection_error(self, e):
        if not self.connected_fut.done():
            self.connected_fut.set_exception(e)
            self.con_state = CONNECTION_BAD

    cdef void _on_greeting_received(self):
        if self.username and self.password:
            self._do_auth(self.username, self.password)
        elif self.fetch_schema:
            self._do_fetch_schema()
        else:
            self._set_connection_ready()

    cdef void _do_auth(self, str username, str password):
        # No extra error handling from Db.execute
        fut = self.execute(
            self._db._auth(self.salt, username, password),
            0
        )

        def on_authorized(f):
            if f.cancelled():
                self._set_connection_error(asyncio.futures.CancelledError())
                return
            e = f.exception()
            if not e:
                logger.debug('Tarantool[%s:%s] Authorized successfully',
                             self.host, self.port)

                if self.fetch_schema:
                    self._do_fetch_schema()
                else:
                    self._set_connection_ready()
            else:
                logger.error('Tarantool[%s:%s] Authorization failed: %s',
                             self.host, self.port, str(e))
                self._set_connection_error(e)

        fut.add_done_callback(on_authorized)

    cdef object _do_fetch_schema(self):
        fut = self.create_future()

        def on_fetch(f):
            if f.cancelled():
                self._set_connection_error(asyncio.futures.CancelledError())
                return
            e = f.exception()
            if not e:
                spaces, indexes = f.result()
                logger.debug('Tarantool[%s:%s] Schema fetch succeeded. '
                             'Spaces: %d, Indexes: %d.',
                             self.host, self.port,
                             len(spaces.body), len(indexes.body))
                self._schema = parse_schema(spaces.schema_id,
                                            spaces.body, indexes.body)
                if self.auto_refetch_schema:
                    # if no refetch, them we should not
                    # send schema_id at all (leave it -1)
                    self._schema_id = self._schema.id
                else:
                    self._schema_id = -1
                self._set_connection_ready()
                fut.set_result(self._schema)
            else:
                logger.error('Tarantool[%s:%s] Schema fetch failed: %s',
                             self.host, self.port, str(e))
                if isinstance(e, asyncio.TimeoutError):
                    e = asyncio.TimeoutError('Schema fetch timeout')
                self._set_connection_error(e)
                fut.set_exception(e)

        self._schema_id = -1
        fut_vspace = self._db.select(SPACE_VSPACE, timeout=0)
        fut_vindex = self._db.select(SPACE_VINDEX, timeout=0)
        gather_fut = asyncio.gather(fut_vspace, fut_vindex,
                                    return_exceptions=False,
                                    loop=self.loop)
        gather_fut.add_done_callback(on_fetch)
        return fut

    cdef void _on_connection_made(self):
        CoreProtocol._on_connection_made(self)

        if self.on_connection_made_cb:
            self.on_connection_made_cb()

    cdef void _on_connection_lost(self, exc):
        CoreProtocol._on_connection_lost(self, exc)

        if self.on_connection_lost_cb:
            self.on_connection_lost_cb(exc)

    cdef uint64_t next_sync(self):
        self._sync += 1
        return self._sync

    def _on_request_timeout(self, waiter):
        cdef Request req

        if waiter.done():
            return

        req = waiter._req
        req.timeout_handle.cancel()
        req.timeout_handle = None
        waiter.set_exception(
            asyncio.TimeoutError(
                '{} exceeded timeout'.format(req.__class__.__name__))
        )

    def _on_request_completed(self, fut):
        cdef Request req = fut._req
        fut._req = None

        if req.timeout_handle is not None:
            req.timeout_handle.cancel()
            req.timeout_handle = None

    cdef object _new_waiter_for_request(self, Request req, float timeout):
        fut = self.create_future()
        fut._req = req  # to be able to retrieve request after done()
        req.waiter = fut

        if timeout < 0:
            timeout = self.request_timeout
        if timeout is not None and timeout > 0:
            req.timeout_handle = \
                self.loop.call_later(timeout,
                                     self._on_request_timeout_cb, fut)
            fut.add_done_callback(self._on_request_completed_cb)
        return fut

    cdef Db _create_db(self):
        return Db.new(self)

    def create_db(self):
        return self._create_db()

    def get_common_db(self):
        return self._db

    cdef object execute(self, Request req, float timeout):
        if self.con_state == CONNECTION_BAD:
            raise TarantoolNotConnectedError('Tarantool is not connected')

        cpython.dict.PyDict_SetItem(self.reqs, req.sync, req)
        self._write(req.build())

        return self._new_waiter_for_request(req, timeout)

    cdef uint32_t transform_iterator(self, iterator) except *:
        if isinstance(iterator, int):
            return iterator
        if isinstance(iterator, Iterator):
            return iterator.value
        if isinstance(iterator, str):
            return Iterator[iterator]
        else:
            raise TypeError('Iterator is of unsupported type '
                            '(asynctnt.Iterator, int, str)')

    cdef uint32_t transform_space(self, space) except *:
        if isinstance(space, str):
            if self._schema is None:
                raise TarantoolSchemaError('Schema not fetched')

            sp = self._schema.get_space(space)
            if sp is None:
                raise TarantoolSchemaError('Space {} not found'.format(space))
            return sp.sid
        return space

    cdef uint32_t transform_index(self, space, index) except *:
        if isinstance(index, str):
            if self._schema is None:
                raise TarantoolSchemaError('Schema not fetched')

            idx = self._schema.get_index(space, index)
            if idx is None:
                raise TarantoolSchemaError(
                    'Index {} for space {} not found'.format(index, space))
            return idx.iid
        return index

    def refetch_schema(self):
        return self._do_fetch_schema()


class Protocol(BaseProtocol, asyncio.Protocol):
    pass
