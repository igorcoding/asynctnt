cimport cython


@cython.final
cdef class UpsertRequest(BaseRequest):
    cdef int encode_body(self, WriteBuffer buffer) except -1:
        return encode_request_update(buffer, self.space, self.space.get_index(0),
                                     self.t, self.operations, <bint> True)
