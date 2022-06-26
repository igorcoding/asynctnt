cdef class SelectRequest(BaseRequest):
    cdef:
        SchemaIndex index
        object key
        uint64_t offset
        uint64_t limit
        uint32_t iterator
