from libc.stdint cimport uint32_t, uint64_t, int64_t


cdef class Request:
    def __cinit__(self):
        self.sync = 0
        self.buf = None
        
    def __init__(self, str encoding, uint64_t sync):
        self.sync = sync
        self.buf = WriteBuffer.new(encoding)
        self.buf.write_header(self.sync, self.op)
        # write body (optional)
        # write length (mandatory): self.buf.write_length()
    
    cdef get_bytes(self):
        return Memory.new(self.buf._buf, self.buf._length).as_bytes()
    

cdef class RequestPing(Request):
    def __cinit__(self):
        self.op = tnt.TP_PING
        
    def __init__(self, str encoding, uint64_t sync):
        Request.__init__(self, encoding, sync)
        self.buf.write_length()


cdef class RequestCall(Request):
    def __cinit__(self):
        self.op = tnt.TP_CALL
        
    def __init__(self, str encoding, uint64_t sync, func_name, args):
        Request.__init__(self, encoding, sync)
        self.buf.encode_request_call(func_name, args)
        self.buf.write_length()
        

cdef class RequestCall16(Request):
    def __cinit__(self):
        self.op = tnt.TP_CALL_16
        
    def __init__(self, str encoding, uint64_t sync, func_name, args):
        Request.__init__(self, encoding, sync)
        self.buf.encode_request_call(func_name, args)
        self.buf.write_length()
        
