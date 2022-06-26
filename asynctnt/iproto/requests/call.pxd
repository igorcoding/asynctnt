cdef class CallRequest(BaseRequest):
    cdef:
        str func_name
        object args
