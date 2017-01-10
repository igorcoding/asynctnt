from libc.stdint cimport uint32_t, uint64_t, int64_t

include "buffer.pxd"
include "tntconst.pxd"


cdef class Request:
    cdef:
        public uint32_t sync
        tp_request_type op
        WriteBuffer buf
        
    cdef make(self)
    cdef make_body(self)
    
    cpdef get_bytes(self)
    
    
cdef class RequestPing(Request):
    pass
