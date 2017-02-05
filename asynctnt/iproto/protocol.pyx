# cython: profile=False

import asyncio
import enum

from asynctnt.exceptions import TarantoolSchemaError

include "const.pxi"

include "buffer.pyx"
include "request.pyx"
include "response.pyx"
include "encdec.pyx"
include "schema.pyx"

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
                 connected_fut,
                 on_connection_made, on_connection_lost,
                 loop,
                 request_timeout=None,
                 encoding='utf-8'):
        CoreProtocol.__init__(self, host, port, encoding)

        self.loop = loop

        self.username = username
        self.password = password
        self.fetch_schema = fetch_schema
        self.request_timeout = request_timeout or 0
        self.connected_fut = connected_fut
        self.on_connection_made_cb = on_connection_made
        self.on_connection_lost_cb = on_connection_lost

        self._on_request_completed_cb = self._on_request_completed
        self._on_request_timeout_cb = self._on_request_timeout

        self._sync = 0
        self._schema = None

        try:
            self.create_future = self.loop.create_future
        except AttributeError:
            self.create_future = self._create_future_fallback

    def _create_future_fallback(self):  # pragma: no cover
        return asyncio.Future(loop=self.loop)

    @property
    def schema(self):
        return self._schema

    cdef void _set_connection_ready(self):
        if not self.connected_fut.done():
            self.connected_fut.set_result(True)
            self.con_state = CONNECTION_FULL

    cdef void _set_connection_error(self, e):
        if not self.connected_fut.done():
            self.connected_fut.set_exception(e)
            self.con_state = CONNECTION_BAD

    cdef void _on_greeting_received(self):
        #print('_on_greeting_received')
        if self.username and self.password:
            self._do_auth(self.username, self.password)
        elif self.fetch_schema:
            self._do_fetch_schema()
        else:
            self._set_connection_ready()

    cdef void _do_auth(self, str username, str password):
        #print('_do_auth')
        fut = self.auth(username, password)

        def on_authorized(f):
            if f.cancelled():
                self._set_connection_error(asyncio.futures.CancelledError())
                return
            e = f.exception()
            if not e:
                logger.debug(
                    'Tarantool[{}:{}] Authorized successfully'.format(
                        self.host, self.port)
                )

                if self.fetch_schema:
                    self._do_fetch_schema()
                else:
                    self._set_connection_ready()
            else:
                logger.error(
                    'Tarantool[{}:{}] Authorization failed: {}'.format(
                        self.host, self.port, str(e))
                )
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
                logger.debug(
                    'Tarantool[{}:{}] Schema fetch succeeded. '
                    'Spaces: {}, Indexes: {}.'.format(
                        self.host, self.port,
                        len(spaces.body), len(indexes.body))
                )
                self._schema = parse_schema(spaces.body, indexes.body)
                self._set_connection_ready()
                fut.set_result(self._schema)
            else:
                logger.error(
                    'Tarantool[{}:{}] Schema fetch failed: {}'.format(
                    self.host, self.port, str(e))
                )
                if isinstance(e, asyncio.TimeoutError):
                    e = asyncio.TimeoutError('Schema fetch timeout')
                self._set_connection_error(e)
                fut.set_exception(e)

        fut_vspace = self.select(SPACE_VSPACE)
        fut_vindex = self.select(SPACE_VINDEX)
        gather_fut = asyncio.gather(fut_vspace, fut_vindex,
                                    return_exceptions=True,
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

    cdef uint64_t _next_sync(self):
        self._sync += 1
        return self._sync

    def _on_request_timeout(self, waiter):
        cdef Request req = waiter._req

        if waiter.done():
            return

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

        timeout = timeout or self.request_timeout
        if timeout is not None and timeout > 0:
            req.timeout_handle = \
                self.loop.call_later(timeout,
                                     self._on_request_timeout_cb, fut)
        req.waiter.add_done_callback(self._on_request_completed_cb)
        return fut

    cdef object _execute(self, Request req, float timeout):
        waiter = self._new_waiter_for_request(req, timeout)

        self.reqs[req.sync] = req
        self._write(req.buf)

        return waiter

    cdef uint32_t _transform_iterator(self, iterator) except *:
        if isinstance(iterator, int):
            return iterator
        if isinstance(iterator, Iterator):
            return iterator.value
        if isinstance(iterator, str):
            return Iterator[iterator]
        else:
            raise TarantoolRequestError('Iterator is of unsupported type')

    cdef uint32_t _transform_space(self, space) except *:
        if isinstance(space, str):
            if self._schema is None:
                raise TarantoolSchemaError('Schema not fetched')

            sp = self._schema.get_space(space)
            if sp is None:
                raise TarantoolSchemaError('Space {} not found'.format(space))
            return sp.sid
        return space

    cdef uint32_t _transform_index(self, space, index) except *:
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

    def ping(self, *, timeout=0):
        return self._execute(
            RequestPing(self.encoding, self._next_sync()),
            timeout
        )

    def auth(self, username, password, *, timeout=0):
        return self._execute(
            RequestAuth(self.encoding, self._next_sync(),
                        self.salt, username, password),
            timeout
        )

    def call16(self, func_name, args=None, *, timeout=0):
        return self._execute(
            RequestCall16(self.encoding, self._next_sync(), func_name, args),
            timeout
        )

    def call(self, func_name, args=None, *, timeout=0):
        return self._execute(
            RequestCall(self.encoding, self._next_sync(), func_name, args),
            timeout
        )

    def eval(self, expression, args=None, *, timeout=0):
        return self._execute(
            RequestEval(self.encoding, self._next_sync(), expression, args),
            timeout
        )

    def select(self, space, key=None, **kwargs):
        offset = kwargs.get('offset', 0)
        limit = kwargs.get('limit', 0xffffffff)
        index = kwargs.get('index', 0)
        iterator = self._transform_iterator(kwargs.get('iterator', 0))
        timeout = kwargs.get('timeout', 0)

        space = self._transform_space(space)
        index = self._transform_index(space, index)

        return self._execute(
            RequestSelect(self.encoding, self._next_sync(),
                          space, index, key, offset, limit, iterator),
            timeout
        )

    def insert(self, space, t, *, replace=False, timeout=0):
        space = self._transform_space(space)

        return self._execute(
            RequestInsert(self.encoding, self._next_sync(),
                          space, t, replace),
            timeout
        )

    def replace(self, space, t, *, timeout=0):
        return self.insert(space, t, replace=True, timeout=timeout)

    def delete(self, space, key, *, **kwargs):
        index = kwargs.get('index', 0)
        timeout = kwargs.get('timeout', 0)

        space = self._transform_space(space)
        index = self._transform_index(space, index)

        return self._execute(
            RequestDelete(self.encoding, self._next_sync(),
                          space, index, key),
            timeout
        )

    def update(self, space, key, operations, **kwargs):
        index = kwargs.get('index', 0)
        timeout = kwargs.get('timeout', 0)

        space = self._transform_space(space)
        index = self._transform_index(space, index)

        return self._execute(
            RequestUpdate(self.encoding, self._next_sync(),
                          space, index, key, operations),
            timeout
        )

    def upsert(self, space, t, operations, **kwargs):
        timeout = kwargs.get('timeout', 0)

        space = self._transform_space(space)

        return self._execute(
            RequestUpsert(self.encoding, self._next_sync(),
                          space, t, operations),
            timeout
        )


class Protocol(BaseProtocol, asyncio.Protocol):
    pass
