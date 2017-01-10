from libc.stdint cimport uint32_t, uint64_t, int64_t

include "const.pxi"
include "buffer.pyx"


cdef class Request:
    def __cinit__(self):
        self.buf = None
        
    def __init__(self, uint32_t sync, tp_request_type op):
        self.sync = sync
        self.op = op
        
    cdef make(self):
        self.buf = WriteBuffer.new()
        self.buf.write_header(self.sync, self.op)
        self.make_body()
        self.buf.write_length()
        
    cdef make_body(self):
        raise NotImplementedError
    
    cpdef get_bytes(self):
        return Memory.new(self.buf._buf, self.buf._length).as_bytes()
    
    
cdef class RequestPing(Request):
    def __init__(self, uint32_t sync):
        super(RequestPing, self).__init__(sync, tntconst.TP_PING)
        
    cdef make_body(self):
        pass
    

cpdef make_request_ping(uint32_t sync):
    # print(sync)
    # return cmake_request_ping(sync)
    r = RequestPing(sync)
    r.make()
    
    # print(Memory.new(r.buf._buf, r.buf._length).as_bytes())
    return r

