cdef class SelectRequest(BaseRequest):
    cdef:
        SchemaIndex index
        object key
        uint64_t offset
        uint64_t limit
        uint32_t iterator

    cdef inline WriteBuffer encode(self, bytes encoding)
    cdef int encode_request_select(self, WriteBuffer buffer,
                                   SchemaSpace space, SchemaIndex index,
                                   key, uint64_t offset, uint64_t limit,
                                   uint32_t iterator) except -1
