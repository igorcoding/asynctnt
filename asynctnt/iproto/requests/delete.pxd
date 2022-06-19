cdef class DeleteRequest(BaseRequest):
    cdef:
        SchemaIndex index
        object key

    cdef inline WriteBuffer encode(self, bytes encoding)
    cdef int encode_request_delete(self, WriteBuffer buffer) except -1
