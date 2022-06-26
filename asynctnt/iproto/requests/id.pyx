cimport cython

DEF IPROTO_VERSION = 3

@cython.final
cdef class IDRequest(BaseRequest):
    cdef int encode_body(self, WriteBuffer buffer) except -1:
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
        # + max arr size
        max_body_len = 1 \
                       + 1 \
                       + 1 \
                       + 1 \
                       + 1 \
                       + 4  # Note: maximum 4 elements in array

        buffer.ensure_allocated(max_body_len)

        p = begin = &buffer._buf[buffer._length]
        p = mp_encode_map(p, body_map_sz)
        p = mp_encode_uint(p, tarantool.IPROTO_VERSION)
        p = mp_encode_uint(p, IPROTO_VERSION)
        p = mp_encode_uint(p, tarantool.IPROTO_FEATURES)
        p = mp_encode_array(p, 3)
        p = mp_encode_uint(p, tarantool.IPROTO_FEATURE_STREAMS)
        p = mp_encode_uint(p, tarantool.IPROTO_FEATURE_TRANSACTIONS)
        p = mp_encode_uint(p, tarantool.IPROTO_FEATURE_ERROR_EXTENSION)

        buffer._length += (p - begin)
