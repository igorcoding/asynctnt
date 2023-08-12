# cython: language_level=3

cimport cpython.dict
from cpython.datetime cimport import_datetime

import_datetime()

import asyncio
import enum

from asynctnt.exceptions import TarantoolNotConnectedError

include "const.pxi"

include "unicodeutil.pyx"
include "schema.pyx"
include "ext.pyx"
include "buffer.pyx"
include "rbuffer.pyx"

include "requests/base.pyx"
include "requests/ping.pyx"
include "requests/call.pyx"
include "requests/eval.pyx"
include "requests/select.pyx"
include "requests/insert.pyx"
include "requests/delete.pyx"
include "requests/update.pyx"
include "requests/upsert.pyx"
include "requests/prepare.pyx"
include "requests/execute.pyx"
include "requests/id.pyx"
include "requests/auth.pyx"
include "requests/streams.pyx"

include "ttuple.pyx"
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
        self.post_con_state = POST_CONNECTION_NONE

        self.connected_fut = connected_fut
        self.on_connection_made_cb = on_connection_made
        self.on_connection_lost_cb = on_connection_lost
        self._closing = False

        self._on_request_completed_cb = self._on_request_completed
        self._on_request_timeout_cb = self._on_request_timeout

        self._reqs = {}
        self._sync = 0
        self._last_stream_id = 0
        self._schema_id = -1
        self._schema = Schema.__new__(Schema, self._schema_id)
        self._schema_fetch_in_progress = False
        self._refetch_schema_future = None
        self._db = self._create_db(<bint> False)
        self.execute = self._execute_bad

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
            self.post_con_state = POST_CONNECTION_NONE
            self.execute = self._execute_bad

    cdef void _on_greeting_received(self):
        self.post_con_state = POST_CONNECTION_ID
        self.execute = self._execute_normal
        self._post_con_state_machine()

    cdef void _post_con_state_machine(self):
        if self.post_con_state == POST_CONNECTION_ID:
            assert self.version is not None
            if self.version >= (2, 10, 0):
                # send <id> request
                self._do_id()
                return
            else:
                # Tarantool does not support id call - call auth request
                self.post_con_state = POST_CONNECTION_AUTH

        if self.post_con_state == POST_CONNECTION_AUTH:
            if self.username and self.password:
                # send <auth> request
                self._do_auth(self.username, self.password)
                return
            else:
                # no need to auth - fetch schema
                self.post_con_state = POST_CONNECTION_SCHEMA

        if self.post_con_state == POST_CONNECTION_SCHEMA:
            if self.fetch_schema:
                self._refetch_schema()
                return
            else:
                # nothing else to do - connection is done
                self.post_con_state = POST_CONNECTION_DONE

        if self.post_con_state == POST_CONNECTION_DONE:
            self._set_connection_ready()
            return

    cdef void _on_response_received(self, const char *buf, uint32_t buf_len):
        cdef:
            PyObject *req_p
            Response response
            BaseRequest req
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

        response_p = cpython.dict.PyDict_GetItem(self._reqs, sync_obj)
        if response_p is NULL:
            logger.warning('sync %d not found', hdr.sync)
            return

        is_chunk = (hdr.code == tarantool.IPROTO_CHUNK)

        response = <Response> response_p
        req = response.request_
        response.sync_ = hdr.sync
        response.schema_id_ = hdr.schema_id
        if not is_chunk:
            response.code_ = hdr.code
            response.return_code_ = hdr.return_code
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
                response_parse_body(buf, buf_len, response, req, is_chunk)
            except Exception as e:
                err = e

        # refetch schema if it is changed
        if self.con_state == CONNECTION_FULL \
                and req.check_schema_change \
                and self.auto_refetch_schema \
                and response.schema_id_ > 0 \
                and response.schema_id_ != self._schema_id:
            self._refetch_schema()

        # returning result
        if is_chunk:
            return

        waiter = req.waiter
        if waiter is None or waiter.done():
            return

        if err is not None:
            response.set_exception(err)
            waiter.set_exception(err)
            return

        if response.is_error():
            err = TarantoolDatabaseError(response.return_code_,
                                         response.errmsg,
                                         response.error)
            response.set_exception(err)
            waiter.set_exception(err)
            return

        waiter.set_result(response)

    cdef void _do_id(self):
        fut = self._db._id(0.0)

        def on_id(f):
            if f.cancelled():
                self._set_connection_error(asyncio.futures.CancelledError())
                return
            e = f.exception()
            if not e:
                logger.debug('Tarantool[%s:%s] identified successfully',
                             self.host, self.port)

                self.post_con_state = POST_CONNECTION_AUTH
                self._post_con_state_machine()
            else:
                logger.error('Tarantool[%s:%s] identification failed: %s',
                             self.host, self.port, str(e))
                self._set_connection_error(e)

        fut.add_done_callback(on_id)

    cdef void _do_auth(self, str username, str password):
        # No extra error handling from Db.execute
        fut = self._db._auth(self.salt, username, password, 0)

        def on_authorized(f):
            if f.cancelled():
                self._set_connection_error(asyncio.futures.CancelledError())
                return
            e = f.exception()
            if not e:
                logger.debug('Tarantool[%s:%s] Authorized successfully',
                             self.host, self.port)

                self.post_con_state = POST_CONNECTION_SCHEMA
                self._post_con_state_machine()
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
                self.post_con_state = POST_CONNECTION_DONE
                self._post_con_state_machine()
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
            BaseRequest req
            Response response
            PyObject *pkey
            PyObject *pvalue
            object key, value
            Py_ssize_t pos

        if self._closing:
            return

        self._closing = True
        self.post_con_state = POST_CONNECTION_NONE
        self.execute = self._execute_bad

        pos = 0
        while cpython.dict.PyDict_Next(self._reqs, &pos, &pkey, &pvalue):
            sync = <uint64_t> <object> pkey
            response = <Response> pvalue
            req = response.request_

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
                    if response is not None:
                        response.set_exception(err)

        if self.on_connection_lost_cb:
            self.on_connection_lost_cb(exc)

        self._reqs = {}  # reset requests map

    cdef inline uint64_t next_sync(self):
        self._sync += 1
        return self._sync

    cdef inline uint64_t next_stream_id(self):
        self._last_stream_id += 1
        return self._last_stream_id

    def _on_request_timeout(self, waiter):
        cdef:
            BaseRequest req
            Response response

        if waiter.done():
            return

        response = waiter._response
        req = response.request_
        req.timeout_handle.cancel()
        req.timeout_handle = None
        waiter.set_exception(
            asyncio.TimeoutError(
                '{} exceeded timeout'.format(req.__class__.__name__))
        )

    def _on_request_completed(self, fut):
        cdef BaseRequest req = (<Response> fut._response).request_
        fut._response = None

        if req.timeout_handle is not None:
            req.timeout_handle.cancel()
            req.timeout_handle = None

    cdef object _new_waiter_for_request(self, Response response, BaseRequest req, float timeout):
        fut = self.create_future()
        req.waiter = fut
        fut._response = response  # to be able to retrieve request after done()

        if timeout < 0:
            timeout = self.request_timeout
        if timeout is not None and timeout > 0:
            req.timeout_handle = \
                self.loop.call_later(timeout,
                                     self._on_request_timeout_cb, fut)
            fut.add_done_callback(self._on_request_completed_cb)
        return fut

    cdef Db _create_db(self, bint gen_stream_id):
        cdef uint64_t stream_id
        if gen_stream_id:
            stream_id = self.next_stream_id()
        else:
            stream_id = 0

        return Db.create(self, stream_id)

    def create_db(self, bint gen_stream_id = False):
        return self._create_db(gen_stream_id)

    def get_common_db(self):
        return self._db

    cdef object _execute_bad(self, BaseRequest req, float timeout):
        raise TarantoolNotConnectedError('Tarantool is not connected')

    cdef object _execute_normal(self, BaseRequest req, float timeout):
        cdef Response response
        response = <Response> Response.__new__(Response)
        response.request_ = req
        response.encoding = self.encoding
        if req.push_subscribe:
            response.init_push()
        cpython.dict.PyDict_SetItem(self._reqs, req.sync, response)
        self._write(req.encode(self.encoding))

        return self._new_waiter_for_request(response, req, timeout)

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

