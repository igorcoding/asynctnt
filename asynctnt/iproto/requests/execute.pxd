cdef class ExecuteRequest(BaseRequest):
    cdef:
        str query
        object args

    cdef inline WriteBuffer encode(self, bytes encoding)
    cdef int encode_request_execute(self, WriteBuffer buffer, str query, args) except -1
