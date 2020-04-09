cdef class SelectRequest(BaseRequest):
    cdef:
        SchemaIndex index
        object key
        uint64_t offset
        uint64_t limit
        uint32_t iterator

    cdef inline WriteBuffer encode(self, bytes encoding):
        cdef WriteBuffer buffer = WriteBuffer.create(encoding)
        buffer.write_header(self.sync, self.op, self.schema_id)
        buffer.encode_request_select(self.space, self.index, self.key,
                                     self.offset, self.limit, self.iterator)
        buffer.write_length()
        return buffer
