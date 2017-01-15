from libc.stdint cimport uint32_t, uint64_t, int64_t


cdef class Request:
    def __cinit__(self):
        self.sync = 0
        self.buf = None
        
    cdef make(self):
        self.buf = WriteBuffer.new()
        self.buf.write_header(self.sync, self.op)
        self.make_body()
        self.buf.write_length()
        
    cdef make_body(self):
        raise NotImplementedError
    
    cdef get_bytes(self):
        return Memory.new(self.buf._buf, self.buf._length).as_bytes()
    

cdef class RequestPing(Request):
    def __cinit__(self):
        self.op = tnt.TP_PING
        
    cdef make_body(self):
        pass
    
    @staticmethod
    cdef RequestPing new():
        cdef RequestPing r
        r = RequestPing.__new__(RequestPing)
        return r
