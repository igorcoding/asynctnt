cdef class PrepareRequest(BaseRequest):
    cdef:
        str query
        uint64_t statement_id
