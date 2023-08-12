cimport cython


@cython.final
cdef class PrepareRequest(BaseRequest):
    cdef int encode_body(self, WriteBuffer buffer) except -1:
        cdef:
            char *begin
            char *p
            uint32_t body_map_sz
            uint32_t max_body_len

            bytes query_temp
            char *query_str
            ssize_t query_len
            uint32_t kind

        body_map_sz = 1
        max_body_len = 0

        query_str = NULL
        query_len = 0

        if self.query is not None:
            query_temp = encode_unicode_string(self.query, buffer._encoding)
            cpython.bytes.PyBytes_AsStringAndSize(query_temp,
                                                  &query_str,
                                                  &query_len)
            # Size description:
            # mp_sizeof_map()
            # + mp_sizeof_uint(TP_SQL_TEXT)
            # + mp_sizeof_str(query)
            # + mp_sizeof_uint(TP_SQL_BIND)
            max_body_len = 1 \
                           + 1 \
                           + mp_sizeof_str(<uint32_t> query_len) \
                           + 1
            kind = tarantool.IPROTO_SQL_TEXT
        else:
            # Size description:
            # mp_sizeof_map()
            # + mp_sizeof_uint(IPROTO_STMT_ID)
            # + mp_sizeof_int(self.statement_id)
            # + mp_sizeof_uint(TP_SQL_BIND)
            max_body_len = 1 \
                           + 1 \
                           + 9 \
                           + 1
            kind = tarantool.IPROTO_STMT_ID

        buffer.ensure_allocated(max_body_len)

        p = begin = &buffer._buf[buffer._length]
        p = mp_encode_map(p, body_map_sz)
        p = mp_encode_uint(p, kind)
        if query_str != NULL:
            p = mp_encode_str(p, query_str, <uint32_t> query_len)
        else:
            p = mp_encode_uint(p, self.statement_id)

        buffer._length += (p - begin)
