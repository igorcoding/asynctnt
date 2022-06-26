cdef class EvalRequest(BaseRequest):
    cdef:
        str expression
        object args
