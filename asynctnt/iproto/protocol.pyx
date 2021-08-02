# cython: language_level=3

cimport cpython.dict

import asyncio
import enum

from asynctnt.exceptions import TarantoolNotConnectedError

include "const.pxi"

include "unicodeutil.pyx"
include "schema.pyx"
include "buffer.pyx"
include "rbuffer.pyx"
include "request.pyx"
include "response.pyx"
include "db.pyx"
include "push.pyx"

include "coreproto.pyx"


class Iterator(enum.IntEnum):
    """
        Available Iterator types
    """
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
        self._closing = False

        self._on_request_completed_cb = self._on_request_completed
        self._on_request_timeout_cb = self._on_request_timeout

        self._reqs = {}
        self._sync = 0
        self._schema_id = -1
        self._schema = Schema.__new__(Schema, self._schema_id)
        self._schema_fetch_in_progress = False
        self._refetch_schema_future = None
        self._db = self._create_db()

        try:
            self.create_future = self.loop.create_future
        except AttributeError:
            self.create_future = self._create_future_fallback

    def _create_future_fallback(self):  # pragma: nocover
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
            self._refetch_schema()
        else:
            self._set_connection_ready()

    cdef void _on_response_received(self, const char *buf, uint32_t buf_len):
        cdef:
            PyObject *req_p
            Request req
            Header hdr
            bint is_chunk
            object waiter
            object sync_obj
            object err

            ssize_t length

        length = response_parse_header(buf, buf_len, &hdr)
        buf_len -= length
        buf = &buf[length]  # skip header

        sync_obj = <object> hdr.sync

        req_p = cpython.dict.PyDict_GetItem(self._reqs, sync_obj)
        if req_p is NULL:
            logger.warning('sync %d not found', hdr.sync)
            return

        is_chunk = (hdr.code == tarantool.IPROTO_CHUNK)

        req = <Request> req_p
        req.response._sync = hdr.sync
        req.response._schema_id = hdr.schema_id
        if not is_chunk:
            req.response._code = hdr.code
            req.response._return_code = hdr.return_code
            cpython.dict.PyDict_DelItem(self._reqs, sync_obj)
        else:
            if not req.push_subscribe:
                # skip request data as no one will be waiting for it
                mp_next(&buf)
                return

        err = None
        if buf != &buf[buf_len]:
            # has body
            try:
                response_parse_body(buf, buf_len, req.response, req, is_chunk)
            except Exception as e:
                err = e

        # refetch schema if it is changed
        if self.con_state == CONNECTION_FULL \
                and req.check_schema_change \
                and self.auto_refetch_schema \
                and req.response._schema_id > 0 \
                and req.response._schema_id != self._schema_id:
            self._refetch_schema()

        # returning result
        if is_chunk:
            return

        waiter = req.waiter
        if waiter is None or waiter.done():
            return

        if err is not None:
            req.response.set_exception(err)
            waiter.set_exception(err)
            return

        if req.response.is_error():
            err = TarantoolDatabaseError(req.response._return_code,
                                         req.response._errmsg)
            req.response.set_exception(err)
            waiter.set_exception(err)
            return

        waiter.set_result(req.response)

    cdef void _do_auth(self, str username, str password):
        # No extra error handling from Db.execute
        fut = self._db._auth(self.salt, username, password, 0, False, False)

        def on_authorized(f):
            if f.cancelled():
                self._set_connection_error(asyncio.futures.CancelledError())
                return
            e = f.exception()
            if not e:
                logger.debug('Tarantool[%s:%s] Authorized successfully',
                             self.host, self.port)

                if self.fetch_schema:
                    self._do_fetch_schema(None)
                else:
                    self._set_connection_ready()
            else:
                logger.error('Tarantool[%s:%s] Authorization failed: %s',
                             self.host, self.port, str(e))
                self._set_connection_error(e)

        fut.add_done_callback(on_authorized)

    cdef void _do_fetch_schema(self, object fut):
        if self._schema_fetch_in_progress:
            return

        self._schema_fetch_in_progress = True

        def on_fetch(f):
            self._schema_fetch_in_progress = False

            if f.cancelled():
                self._set_connection_error(asyncio.futures.CancelledError())
                return
            e = f.exception()
            if not e:
                if fut is not None and fut.cancelled():
                    # if the caller has cancelled waiting
                    return

                spaces, indexes = f.result()
                logger.debug('Tarantool[%s:%s] Schema fetch succeeded. '
                             'Version: %d, Spaces: %d, Indexes: %d.',
                             self.host, self.port,
                             spaces.schema_id, len(spaces), len(indexes))
                try:
                    self._schema = Schema.parse(spaces.schema_id,
                                                spaces, indexes)
                except Exception as e:
                    logger.exception(e)
                    logger.error('Error happened while parsing schema. '
                                 'Space, fields and index names currently '
                                 'not working. Please file an issue at '
                                 'https://github.com/igorcoding/asynctnt')
                    self.auto_refetch_schema = False
                    self.fetch_schema = False
                    self._schema_id = -1
                    self._set_connection_ready()
                    if fut is not None and not fut.done():
                        fut.set_result(None)
                    return

                self._schema_id = self._schema.id
                self._set_connection_ready()
                if fut is not None and not fut.done():
                    fut.set_result(self._schema)
            else:
                if self._closing:
                    # show a diag message rather than Lost connection to Tarantool when disconnected (#19)
                    logger.debug('Schema fetch stopped: connection is closed')
                    return

                logger.error('Tarantool[%s:%s] Schema fetch failed: %s',
                             self.host, self.port, str(e))
                if isinstance(e, asyncio.TimeoutError):
                    e = asyncio.TimeoutError('Schema fetch timeout')
                self._set_connection_error(e)
                if fut is not None and not fut.done():
                    fut.set_exception(e)

        fut_vspace = self._db.select(SPACE_VSPACE, timeout=0,
                                     check_schema_change=False)
        fut_vindex = self._db.select(SPACE_VINDEX, timeout=0,
                                     check_schema_change=False)
        gather_fut = asyncio.gather(fut_vspace, fut_vindex,
                                    return_exceptions=False)
        gather_fut.add_done_callback(on_fetch)

    cdef void _on_connection_made(self):
        CoreProtocol._on_connection_made(self)

        if self.on_connection_made_cb:
            self.on_connection_made_cb()

    cdef void _on_connection_lost(self, exc):
        cdef:
            Request req
            PyObject *pkey
            PyObject *pvalue
            object key, value
            Py_ssize_t pos

        if self._closing:
            return

        self._closing = True

        pos = 0
        while cpython.dict.PyDict_Next(self._reqs, &pos, &pkey, &pvalue):
            sync = <uint64_t> <object> pkey
            req = <Request> pvalue

            waiter = req.waiter
            if waiter and not waiter.done():
                err = None
                if exc is None:
                    err = TarantoolNotConnectedError(
                        'Lost connection to Tarantool'
                    )
                elif isinstance(exc, (ConnectionRefusedError,
                                      ConnectionResetError)):
                    err = TarantoolNotConnectedError(
                        'Lost connection to Tarantool: {}: {}'.format(
                            exc.__class__.__name__, str(exc))
                    )
                elif exc is ConnectionRefusedError \
                        or exc is ConnectionResetError:
                    err = TarantoolNotConnectedError(
                        'Lost connection to Tarantool: {}'.format(
                            exc.__name__)
                    )
                else:
                    err = exc

                if err is not None:
                    waiter.set_exception(err)
                    if req.response is not None:
                        req.response.set_exception(err)

        if self.on_connection_lost_cb:
            self.on_connection_lost_cb(exc)

        self._reqs = {}  # reset requests map

    cdef inline uint64_t next_sync(self):
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
        req.response = Response.__new__(Response, self.encoding,
                                        req.push_subscribe)

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

    cdef object execute(self, Request req, WriteBuffer buf, float timeout):
        if self.con_state == CONNECTION_BAD:
            raise TarantoolNotConnectedError('Tarantool is not connected')

        cpython.dict.PyDict_SetItem(self._reqs, req.sync, req)
        self._write(buf)

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

    cdef object _refetch_schema(self):
        if self._refetch_schema_future is not None and not self._refetch_schema_future.done():
            self._schema_fetch_in_progress = False
            self._refetch_schema_future.cancel()

        self._refetch_schema_future = self.create_future()
        self._do_fetch_schema(self._refetch_schema_future)

        return self._refetch_schema_future

    def refetch_schema(self):
        return self._refetch_schema()


class Protocol(BaseProtocol, asyncio.Protocol):
    pass


TarantoolTuple = <object> tupleobj.AtntTuple_InitTypes()
