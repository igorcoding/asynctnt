from libc.stdint cimport uint32_t, uint64_t, int64_t

cimport tnt

cdef class Request:
    cdef:
        uint64_t sync
        tnt.tp_request_type op
        WriteBuffer buf
        
    cdef make(self)
    cdef make_body(self)
    
    cdef get_bytes(self)
    
    
cdef class RequestPing(Request):
    @staticmethod
    cdef RequestPing new()
