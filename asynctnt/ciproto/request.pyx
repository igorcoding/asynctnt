from libc.stdint cimport uint32_t, uint64_t, int64_t


cdef class Request:
    def __cinit__(self):
        self.sync = 0
        self.buf = None
        self.waiter = None
        self.timeout_handle = None
        
    def __init__(self, tnt.tp_request_type op, str encoding, uint64_t sync):
        self.op = op
        self.sync = sync
        self.buf = WriteBuffer.new(encoding)
        self.buf.write_header(self.sync, self.op)
        # write body (optional)
        # write length (mandatory): self.buf.write_length()
    
    cdef get_bytes(self):
        return Memory.new(self.buf._buf, self.buf._length).as_bytes()
    

cdef class RequestPing(Request):
    def __init__(self, str encoding, uint64_t sync):
        Request.__init__(self, tnt.TP_PING, encoding, sync)
        self.buf.write_length()


cdef class RequestCall(Request):
    def __init__(self, str encoding, uint64_t sync, func_name, args):
        Request.__init__(self, tnt.TP_CALL, encoding, sync)
        self.buf.encode_request_call(func_name, args)
        self.buf.write_length()
        

cdef class RequestCall16(Request):
    def __init__(self, str encoding, uint64_t sync, func_name, args):
        Request.__init__(self, tnt.TP_CALL_16, encoding, sync)
        self.buf.encode_request_call(func_name, args)
        self.buf.write_length()
        
