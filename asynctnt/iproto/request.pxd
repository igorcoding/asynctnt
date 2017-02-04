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

cdef class RequestCall16(Request):
    pass

cdef class RequestEval(Request):
    pass

cdef class RequestSelect(Request):
    pass

cdef class RequestInsert(Request):
    pass

cdef class RequestDelete(Request):
    pass

cdef class RequestUpdate(Request):
    pass

cdef class RequestUpsert(Request):
    pass

cdef class RequestAuth(Request):
    cdef bytes sha1(self, tuple values)
    cdef bytes strxor(self, bytes hash1, bytes scramble)
