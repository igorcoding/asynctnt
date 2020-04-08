cdef class UpdateRequest(BaseRequest):
    cdef:
        SchemaIndex index
        object key
        list operations

    cdef inline WriteBuffer encode(self, bytes encoding):
        cdef WriteBuffer buffer = WriteBuffer.new(encoding)
        buffer.write_header(self.sync, self.op, self.schema_id)
        buffer.encode_request_update(self.space, self.index, self.key,
                                     self.operations)
        buffer.write_length()
        return buffer
