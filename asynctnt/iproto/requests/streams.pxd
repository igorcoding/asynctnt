cdef class BeginRequest(BaseRequest):
    cdef:
        uint32_t isolation
        double tx_timeout

    cdef inline WriteBuffer encode(self, bytes encoding):
        cdef WriteBuffer buffer = WriteBuffer.create(encoding)
        buffer.write_header(self.sync, self.op, self.schema_id, self.stream_id)
        self.encode_request(buffer)
        buffer.write_length()
        return buffer

    cdef inline int encode_request(self, WriteBuffer buffer) except -1:
        cdef:
            char *p
            char *begin
            uint32_t body_map_sz
            uint32_t max_body_len

        body_map_sz = 2
        # Size description:
        max_body_len = mp_sizeof_map(body_map_sz) \
                        + mp_sizeof_uint(tarantool.IPROTO_TXN_ISOLATION) \
                        + mp_sizeof_uint(self.isolation) \
                        + mp_sizeof_uint(tarantool.IPROTO_TIMEOUT) \
                        + mp_sizeof_double(self.tx_timeout)

        buffer.ensure_allocated(max_body_len)

        p = begin = &buffer._buf[buffer._length]
        p = mp_encode_map(p, body_map_sz)
        p = mp_encode_uint(p, tarantool.IPROTO_TXN_ISOLATION)
        p = mp_encode_uint(p, self.isolation)
        p = mp_encode_uint(p, tarantool.IPROTO_TIMEOUT)
        p = mp_encode_double(p, self.tx_timeout)

        buffer._length += (p - begin)

cdef class CommitRequest(BaseRequest):
    cdef inline WriteBuffer encode(self, bytes encoding):
        cdef WriteBuffer buffer = WriteBuffer.create(encoding)
        buffer.write_header(self.sync, self.op, self.schema_id, self.stream_id)
        buffer.write_length()
        return buffer

cdef class RollbackRequest(BaseRequest):
    cdef inline WriteBuffer encode(self, bytes encoding):
        cdef WriteBuffer buffer = WriteBuffer.create(encoding)
        buffer.write_header(self.sync, self.op, self.schema_id, self.stream_id)
        buffer.write_length()
        return buffer
