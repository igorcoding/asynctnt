cimport cython

cimport asynctnt.iproto.tarantool as tarantool


@cython.final
cdef class CallRequest(BaseRequest):
    cdef int encode_body(self, WriteBuffer buffer) except -1:
        cdef:
            char *begin
            char *p
            uint32_t body_map_sz
            uint32_t max_body_len

            bytes func_name_temp
            char *func_name_str
            ssize_t func_name_len

        func_name_str = NULL
        func_name_len = 0

        func_name_temp = encode_unicode_string(self.func_name, buffer._encoding)
        cpython.bytes.PyBytes_AsStringAndSize(func_name_temp,
                                              &func_name_str,
                                              &func_name_len)
        body_map_sz = 2
        # Size description:
        # mp_sizeof_map()
        # + mp_sizeof_uint(TP_FUNCTION)
        # + mp_sizeof_str(func_name)
        # + mp_sizeof_uint(TP_TUPLE)
        max_body_len = 1 \
                       + 1 \
                       + mp_sizeof_str(<uint32_t> func_name_len) \
                       + 1

        buffer.ensure_allocated(max_body_len)

        p = begin = &buffer._buf[buffer._length]
        p = mp_encode_map(p, body_map_sz)
        p = mp_encode_uint(p, tarantool.IPROTO_FUNCTION_NAME)
        p = mp_encode_str(p, func_name_str, <uint32_t> func_name_len)

        p = mp_encode_uint(p, tarantool.IPROTO_TUPLE)
        buffer._length += (p - begin)
        p = encode_key_sequence(buffer, p, self.args, None, False)
