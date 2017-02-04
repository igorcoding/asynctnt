import hashlib

cimport cpython.bytes
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
    def __init__(self, str encoding, uint64_t sync,
                 func_name, args):
        Request.__init__(self, tnt.TP_CALL, encoding, sync)
        self.buf.encode_request_call(func_name, args)
        self.buf.write_length()
        

cdef class RequestCall16(Request):
    def __init__(self, str encoding, uint64_t sync,
                 func_name, args):
        Request.__init__(self, tnt.TP_CALL_16, encoding, sync)
        self.buf.encode_request_call(func_name, args)
        self.buf.write_length()
        

cdef class RequestEval(Request):
    def __init__(self, str encoding, uint64_t sync,
                 expression, args):
        Request.__init__(self, tnt.TP_EVAL, encoding, sync)
        self.buf.encode_request_eval(expression, args)
        self.buf.write_length()


cdef class RequestSelect(Request):
    def __init__(self, str encoding, uint64_t sync,
                 uint32_t space, uint32_t index, list key,
                 uint64_t offset, uint64_t limit, uint32_t iterator):
        Request.__init__(self, tnt.TP_SELECT, encoding, sync)
        self.buf.encode_request_select(space, index, key,
                                       offset, limit, iterator)
        self.buf.write_length()
        
        
cdef class RequestInsert(Request):
    def __init__(self, str encoding, uint64_t sync,
                 uint32_t space, list t, bint replace):
        op = tnt.TP_INSERT if not replace else tnt.TP_REPLACE
        Request.__init__(self, op, encoding, sync)
        self.buf.encode_request_insert(space, t)
        self.buf.write_length()
        

cdef class RequestDelete(Request):
    def __init__(self, str encoding, uint64_t sync,
                 uint32_t space, uint32_t index, list key):
        Request.__init__(self, tnt.TP_DELETE, encoding, sync)
        self.buf.encode_request_delete(space, index, key)
        self.buf.write_length()
        
        
cdef class RequestUpdate(Request):
    def __init__(self, str encoding, uint64_t sync,
                 uint32_t space, uint32_t index,
                 list key, list operations):
        Request.__init__(self, tnt.TP_UPDATE, encoding, sync)
        self.buf.encode_request_update(space, index, key, operations)
        self.buf.write_length()
        
        
cdef class RequestUpsert(Request):
    def __init__(self, str encoding, uint64_t sync,
                 uint32_t space,
                 list t, list operations):
        Request.__init__(self, tnt.TP_UPSERT, encoding, sync)
        self.buf.encode_request_upsert(space, t, operations)
        self.buf.write_length()


cdef class RequestAuth(Request):
    def __init__(self, str encoding, uint64_t sync,
                 salt, username, password):
        cdef:
            bytes username_bytes
            bytes password_bytes
            bytes scramble
            
        Request.__init__(self, tnt.TP_AUTH, encoding, sync)
        if isinstance(username, bytes):
            username_bytes = username
        else:
            username_bytes = username.encode(encoding)
        
        if isinstance(password, bytes):
            password_bytes = password
        else:
            password_bytes = password.encode(encoding)
            
        hash1 = self.sha1((password_bytes,))
        hash2 = self.sha1((hash1,))
        scramble = self.sha1((salt, hash2))
        scramble = self.strxor(hash1, scramble)
        
        self.buf.encode_request_auth(username_bytes, scramble)
        self.buf.write_length()

    cdef bytes sha1(self, tuple values):
        cdef object sha = hashlib.sha1()
        for i in values:
            if i is not None:
                sha.update(i)
        return sha.digest()

    cdef bytes strxor(self, bytes hash1, bytes scramble):
        cdef:
            char* hash1_str
            ssize_t hash1_len
            
            char* scramble_str
            ssize_t scramble_len
        cpython.bytes.PyBytes_AsStringAndSize(hash1,
                                              &hash1_str, &hash1_len)
        cpython.bytes.PyBytes_AsStringAndSize(scramble,
                                              &scramble_str, &scramble_len)
        for i in range(scramble_len):
            scramble_str[i] = hash1_str[i] ^ scramble_str[i]
        return scramble
