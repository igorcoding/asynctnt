cdef class ExecuteRequest(BaseRequest):
    cdef:
        str query
        uint64_t statement_id
        object args

    cdef inline WriteBuffer encode(self, bytes encoding)
    cdef int encode_request_execute(self, WriteBuffer buffer) except -1
