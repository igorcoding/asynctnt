cdef class EvalRequest(BaseRequest):
    cdef:
        str expression
        object args

    cdef inline WriteBuffer encode(self, bytes encoding)
    cdef int encode_request_eval(self, WriteBuffer buffer, str expression, args) except -1
