cdef class ExecuteRequest(BaseRequest):
    cdef:
        str query
        object args

    cdef inline WriteBuffer encode(self, bytes encoding):
        cdef WriteBuffer buffer = WriteBuffer.new(encoding)
        buffer.write_header(self.sync, self.op, self.schema_id)
        buffer.encode_request_sql(self.query, self.args)
        buffer.write_length()
        return buffer
