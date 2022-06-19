cdef class UpsertRequest(BaseRequest):
    cdef:
        object t
        list operations

    cdef inline WriteBuffer encode(self, bytes encoding)
