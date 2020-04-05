cdef class EvalRequest(BaseRequest):
    cdef:
        str expression
        object args

    cdef inline WriteBuffer encode(self, bytes encoding):
        cdef WriteBuffer buffer = WriteBuffer.new(encoding)
        buffer.write_header(self.sync, self.op, self.schema_id)
        buffer.encode_request_eval(self.expression, self.args)
        buffer.write_length()
        return buffer
