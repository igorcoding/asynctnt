from libc.stdint cimport uint32_t, uint64_t, int64_t

cimport tnt

cdef class Request:
    cdef:
        uint64_t sync
        tnt.tp_request_type op
        WriteBuffer buf
    
    cdef get_bytes(self)
    
    
cdef class RequestPing(Request):
    pass

cdef class RequestCall(Request):
    pass
