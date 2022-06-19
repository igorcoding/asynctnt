cdef class CallRequest(BaseRequest):
    cdef:
        str func_name
        object args

    cdef inline WriteBuffer encode(self, bytes encoding)
    cdef int encode_request_call(self, WriteBuffer buffer, str func_name, args) except -1