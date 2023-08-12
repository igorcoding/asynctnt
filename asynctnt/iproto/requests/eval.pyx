cimport cython


@cython.final
cdef class EvalRequest(BaseRequest):
    cdef int encode_body(self, WriteBuffer buffer) except -1:
        cdef:
            char *begin
            char *p
            uint32_t body_map_sz
            uint32_t max_body_len

            bytes expression_temp
            char *expression_str
            ssize_t expression_len

        expression_str = NULL
        expression_len = 0

        expression_temp = encode_unicode_string(self.expression, buffer._encoding)
        cpython.bytes.PyBytes_AsStringAndSize(expression_temp,
                                              &expression_str,
                                              &expression_len)
        body_map_sz = 2
        # Size description:
        # mp_sizeof_map()
        # + mp_sizeof_uint(TP_EXPRESSION)
        # + mp_sizeof_str(expression)
        # + mp_sizeof_uint(TP_TUPLE)
        max_body_len = 1 \
                       + 1 \
                       + mp_sizeof_str(<uint32_t> expression_len) \
                       + 1

        buffer.ensure_allocated(max_body_len)

        p = begin = &buffer._buf[buffer._length]
        p = mp_encode_map(p, body_map_sz)
        p = mp_encode_uint(p, tarantool.IPROTO_EXPR)
        p = mp_encode_str(p, expression_str, <uint32_t> expression_len)

        p = mp_encode_uint(p, tarantool.IPROTO_TUPLE)
        buffer._length += (p - begin)
        p = encode_key_sequence(buffer, p, self.args, None, False)
