cdef class CallRequest(BaseRequest):
    cdef:
        str func_name
        object args

    cdef inline WriteBuffer encode(self, bytes encoding):
        cdef WriteBuffer buffer = WriteBuffer.create(encoding)
        buffer.write_header(self.sync, self.op, self.schema_id)
        buffer.encode_request_call(self.func_name, self.args)
        buffer.write_length()
        return buffer
