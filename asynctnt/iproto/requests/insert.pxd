cdef class InsertRequest(BaseRequest):
    cdef:
        object t

    cdef inline WriteBuffer encode(self, bytes encoding)
    cdef int encode_request_insert(self, WriteBuffer buffer, SchemaSpace space, t) except -1
