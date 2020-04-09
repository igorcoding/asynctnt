cimport cython

@cython.final
cdef class UpsertRequest(BaseRequest):

    cdef inline WriteBuffer encode(self, bytes encoding):
        cdef WriteBuffer buffer = WriteBuffer.create(encoding)
        buffer.write_header(self.sync, self.op, self.schema_id)
        encode_request_update(buffer, self.space, self.space.get_index(0), 
                              self.t, self.operations, True)
        buffer.write_length()
        return buffer
