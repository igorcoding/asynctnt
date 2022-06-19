cimport cython

@cython.final
cdef class ExecuteRequest(BaseRequest):

    cdef inline WriteBuffer encode(self, bytes encoding):
        cdef WriteBuffer buffer = WriteBuffer.create(encoding)
        buffer.write_header(self.sync, self.op, self.schema_id)
        self.encode_request_execute(buffer, self.query, self.args)
        buffer.write_length()
        return buffer

    cdef int encode_request_execute(self, WriteBuffer buffer, str query, args) except -1:
        cdef:
            char *begin
            char *p
            uint32_t body_map_sz
            uint32_t max_body_len

            bytes query_temp
            char *query_str
            ssize_t query_len

        query_str = NULL
        query_len = 0

        query_temp = encode_unicode_string(query, buffer._encoding)
        cpython.bytes.PyBytes_AsStringAndSize(query_temp,
                                              &query_str,
                                              &query_len)
        body_map_sz = 2
        # Size description:
        # mp_sizeof_map()
        # + mp_sizeof_uint(TP_SQL_TEXT)
        # + mp_sizeof_str(query)
        # + mp_sizeof_uint(TP_SQL_BIND)
        max_body_len = 1 \
                       + 1 \
                       + mp_sizeof_str(<uint32_t> query_len) \
                       + 1

        buffer.ensure_allocated(max_body_len)

        p = begin = &buffer._buf[buffer._length]
        p = mp_encode_map(p, body_map_sz)
        p = mp_encode_uint(p, tarantool.IPROTO_SQL_TEXT)
        p = mp_encode_str(p, query_str, <uint32_t> query_len)

        p = mp_encode_uint(p, tarantool.IPROTO_SQL_BIND)
        buffer._length += (p - begin)
        # TODO: replace with custom encoder
        # TODO: need to simultaneously encode ordinal and named params
        p = encode_key_sequence(buffer, p, args, None, False)
