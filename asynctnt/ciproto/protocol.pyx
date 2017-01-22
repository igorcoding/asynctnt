# cython: profile=True

import asyncio

include "const.pxi"

include "buffer.pyx"
include "request.pyx"
include "response.pyx"
include "encdec.pyx"
include "schema.pyx"

include "coreproto.pyx"


cdef class BaseProtocol(CoreProtocol):
    def __init__(self, host, port,
                 username, password,
                 fetch_schema,
                 connected_fut, on_connection_lost, loop,
                 encoding='utf-8'):
        CoreProtocol.__init__(self, host, port, encoding)
        
        self.loop = loop
        
        self.username = username
        self.password = password
        self.fetch_schema = fetch_schema
        self.connected_fut = connected_fut
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
        self.connected_fut.set_result(True)
        self.con_state = CONNECTION_FULL
        
    cdef void _set_connection_error(self, e):
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
        fut = self.auth(username, password)
        
        def on_authorized(f):
            if f.cancelled():
                self._set_connection_error(asyncio.futures.CancelledError())
                return
            e = f.exception()
            if not e:
                print('Tarantool[{}:{}] Authorized successfully'.format(self.host, self.port))
                
                if self.fetch_schema:
                    self._do_fetch_schema()
                else:
                    self._set_connection_ready()
            else:
                print('Tarantool[{}:{}] Authorization failed'.format(self.host, self.port))
                self._set_connection_error(e)
        
        fut.add_done_callback(on_authorized)
            
    cdef void _do_fetch_schema(self):
        fut_vspace = self.select(SPACE_VSPACE)
        fut_vindex = self.select(SPACE_VINDEX)
        
        def on_fetch(f):
            if f.cancelled():
                self._set_connection_error(asyncio.futures.CancelledError())
                return
            e = f.exception()
            if not e:
                spaces, indexes = f.result()
                print('Tarantool[{}:{}] Schema fetch succeeded. '
                      'Spaces: {}, Indexes: {}.'.format(
                    self.host, self.port, len(spaces.body), len(indexes.body)))
                self._schema = parse_schema(spaces.body, indexes.body)
                self._set_connection_ready()
            else:
                print('Tarantool[{}:{}] Schema fetch failed'.format(
                    self.host, self.port))
                if isinstance(e, asyncio.TimeoutError):
                    self._set_connection_error(
                        asyncio.TimeoutError('Schema fetch timeout'))
                else:
                    self._set_connection_error(e)
        
        fut = asyncio.gather(fut_vspace, fut_vindex,
                             return_exceptions=True,
                             loop=self.loop)
        fut.add_done_callback(on_fetch)
        
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
        
        # timeout = timeout
        if timeout is not None and timeout > 0:
            req.timeout_handle = \
                self.loop.call_later(timeout, self._on_request_timeout_cb, fut)
        req.waiter.add_done_callback(self._on_request_completed_cb)
        return fut

    cdef object _execute(self, Request req, float timeout):
        if not self._is_connected():
            raise NotConnectedError('Tarantool is not connected')
        
        waiter = self._new_waiter_for_request(req, timeout)
        
        self.reqs[req.sync] = req
        self._write(req.buf)
        
        return waiter
    
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
    
    def select(self, space, key=None, *, **kwargs):
        offset = kwargs.get('offset', 0)
        limit = kwargs.get('limit', 0xffffffff)
        index = kwargs.get('index', 0)
        iterator = kwargs.get('iterator', 0)
        timeout = kwargs.get('timeout', 0)
        
        if isinstance(space, str):
            raise NotImplementedError
        
        if isinstance(index, str):
            raise NotImplementedError
    
        return self._execute(
            RequestSelect(self.encoding, self._next_sync(),
                          space, index, key, offset, limit, iterator),
            timeout
        )
    
class Protocol(BaseProtocol, asyncio.Protocol):
    pass
