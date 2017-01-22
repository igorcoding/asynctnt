from libc.stdint cimport uint32_t, uint64_t, int64_t

cimport tnt

cdef class Request:
    cdef:
        uint64_t sync
        tnt.tp_request_type op
        WriteBuffer buf
        object waiter
        object timeout_handle
    
    cdef get_bytes(self)
    
    
cdef class RequestPing(Request):
    pass

cdef class RequestCall(Request):
    pass
