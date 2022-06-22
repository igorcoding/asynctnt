cimport cython

DEF IPROTO_VERSION = 3

@cython.final
cdef class IDRequest(BaseRequest):

    cdef inline WriteBuffer encode(self, bytes encoding):
        cdef WriteBuffer buffer = WriteBuffer.create(encoding)
        buffer.write_header(self.sync, self.op, self.schema_id)
        self.encode_request(buffer)
        buffer.write_length()
        return buffer

    cdef int encode_request(self, WriteBuffer buffer) except -1:
        cdef:
            char *p
            char *begin
            uint32_t body_map_sz
            uint32_t max_body_len

        body_map_sz = 2
        # Size description:
        # mp_sizeof_map(body_map_sz)
        # + mp_sizeof_uint(tarantool.IPROTO_VERSION)
        # + mp_sizeof_uint(IPROTO_VERSION)
        # + mp_sizeof_uint(tarantool.IPROTO_FEATURES)
        # + mp_sizeof_array(0)
        # + arr size
        max_body_len = 1 \
                       + 1 \
                       + 1 \
                       + 1 \
                       + 1 \
                       + 0

        buffer.ensure_allocated(max_body_len)

        p = begin = &buffer._buf[buffer._length]
        p = mp_encode_map(p, body_map_sz)
        p = mp_encode_uint(p, tarantool.IPROTO_VERSION)
        p = mp_encode_uint(p, IPROTO_VERSION)
        p = mp_encode_uint(p, tarantool.IPROTO_FEATURES)
        p = mp_encode_array(p, 0)
        # p = mp_encode_uint(p, tarantool.IPROTO_FEATURE_ERROR_EXTENSION)

        buffer._length += (p - begin)
