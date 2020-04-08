cdef class UpsertRequest(BaseRequest):
    cdef:
        object t
        list operations

    cdef inline WriteBuffer encode(self, bytes encoding):
        cdef WriteBuffer buffer = WriteBuffer.new(encoding)
        buffer.write_header(self.sync, self.op, self.schema_id)
        buffer.encode_request_upsert(self.space, self.t, self.operations)
        buffer.write_length()
        return buffer
