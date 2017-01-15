# cython: profile=False

import asyncio

include "const.pxi"

include "buffer.pyx"
include "response.pyx"
include "request.pyx"
include "encdec.pyx"

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
        
        self._sync = 0
        
        try:
            self.create_future = self.loop.create_future
        except AttributeError:
            self.create_future = self._create_future_fallback
    
    def _create_future_fallback(self):  # pragma: no cover
        return asyncio.Future(loop=self.loop)
    
    cdef _set_connection_ready(self):
        self.connected_fut.set_result(True)
        self.con_state = CONNECTION_FULL
    
    cdef _on_greeting_received(self):
        if self.username and self.password:
            self._do_auth(self.username, self.password)
        elif self.fetch_schema:
            self._do_fetch_schema()
        else:
            self._set_connection_ready()

    cdef _do_auth(self, str username, str password):
        # TODO: make auth
        if self.fetch_schema:
            self._do_fetch_schema()
        else:
            self._set_connection_ready()
            
    cdef _do_fetch_schema(self):
        self._set_connection_ready()
        
    cdef _on_connection_lost(self, exc):
        CoreProtocol._on_connection_lost(self, exc)
        
        if self.on_connection_lost_cb:
            self.on_connection_lost_cb(exc)
            
    cdef _next_sync(self):
        self._sync += 1
        return self._sync

    cdef _execute(self, Request req, float timeout):
        if not self._is_connected():
            raise NotConnectedError('Tarantool is not connected')
        
        waiter = self.create_future()
        if timeout and timeout > 0:
            # Client should wait the special timeout-ed future (wrapping waiter)
            fut = asyncio.ensure_future(
                asyncio.wait_for(waiter, timeout=timeout, loop=self.loop),
                loop=self.loop
            )
        else:
            # Client should wait the waiter
            fut = waiter
        
        req.sync = self._next_sync()
        req.make()
        self.reqs[req.sync] = waiter
        self._write(req.buf)
        
        return fut
    
    def ping(self, timeout=0):
        return self._execute(RequestPing.new(), timeout=timeout)
    
    
class Protocol(BaseProtocol, asyncio.Protocol):
    pass
