cimport cython
from libc.stdint cimport uint64_t, int64_t

cimport tnt


@cython.final
@cython.freelist(_BUFFER_FREELIST_SIZE)
cdef class Request:
    @staticmethod
    cdef inline Request new(tnt.tp_request_type op,
                            uint64_t sync, int64_t schema_id,
                            WriteBuffer buf_body):
        cdef Request req
        req = Request.__new__(Request)
        req.op = op
        req.sync = sync
        req.schema_id = schema_id
        req.buf_body = buf_body
        req.waiter = None
        req.timeout_handle = None
        return req

    cdef WriteBuffer build(self):
        cdef WriteBuffer buf
        buf = WriteBuffer.new()
        buf.write_header(self.sync, self.op, self.schema_id)
        if self.buf_body is not None:
            buf.write_buffer(self.buf_body)
        buf.write_length()
        return buf

    def __repr__(self):
        return '<Request op={}, sync={}, schema_id={}, body_len={}>'.format(
            self.op,
            self.sync,
            self.schema_id,
            self.buf_body.len()
        )
