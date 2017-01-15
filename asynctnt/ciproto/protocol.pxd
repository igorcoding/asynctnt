include "const.pxi"

include "cmsgpuck.pxd"
include "python.pxd"

include "buffer.pxd"
include "response.pxd"
include "request.pxd"
include "encdec.pxd"

include "coreproto.pxd"


cdef class BaseProtocol(CoreProtocol):
    cdef:
        object loop
        str username
        str password
        bint fetch_schema
        object connected_fut
        object on_connected_lost_cb
        
        uint64_t _sync
        
    cdef _set_connection_ready(self)

    cdef _do_auth(self, str username, str password)
    cdef _do_fetch_schema(self)
    
    cdef _next_sync(self)
    cdef _execute(self, Request req, float timeout)
