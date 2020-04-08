cdef class DeleteRequest(BaseRequest):
    cdef:
        SchemaIndex index
        object key

    cdef inline WriteBuffer encode(self, bytes encoding):
        cdef WriteBuffer buffer = WriteBuffer.new(encoding)
        buffer.write_header(self.sync, self.op, self.schema_id)
        buffer.encode_request_delete(self.space, self.index, self.key)
        buffer.write_length()
        return buffer
