cdef class ExecuteRequest(BaseRequest):
    cdef:
        str query
        uint64_t statement_id
        object args
