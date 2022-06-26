cdef class UpsertRequest(BaseRequest):
    cdef:
        object t
        list operations
