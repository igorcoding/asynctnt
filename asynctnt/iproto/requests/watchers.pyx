cimport cython


@cython.final
cdef class WatchRequest(BaseRequest):
    cdef int encode_body(self, WriteBuffer buffer) except -1:
        cdef:
            char *p
            char *begin
            uint32_t body_map_sz
            uint32_t max_body_len

            bytes key_temp
            char *key_str
            ssize_t key_len

        body_map_sz = 1

        key_temp = encode_unicode_string(self.key, buffer._encoding)
        cpython.bytes.PyBytes_AsStringAndSize(key_temp,
                                              &key_str,
                                              &key_len)


        # Size description:
        max_body_len = mp_sizeof_map(body_map_sz) \
                       + mp_sizeof_uint(tarantool.IPROTO_EVENT_KEY) \
                       + mp_sizeof_str(<uint32_t> key_len)

        buffer.ensure_allocated(max_body_len)

        p = begin = &buffer._buf[buffer._length]
        p = mp_encode_map(p, body_map_sz)
        p = mp_encode_uint(p, tarantool.IPROTO_EVENT_KEY)
        p = mp_encode_str(p, key_str, <uint32_t> key_len)

        buffer._length += (p - begin)
