cdef class InsertRequest(BaseRequest):
    cdef:
        object t

    cdef inline WriteBuffer encode(self, bytes encoding):
        cdef WriteBuffer buffer = WriteBuffer.create(encoding)
        buffer.write_header(self.sync, self.op, self.schema_id)
        buffer.encode_request_insert(self.space, self.t)
        buffer.write_length()
        return buffer
