cimport cython


@cython.final
cdef class DeleteRequest(BaseRequest):

    cdef int encode_body(self, WriteBuffer buffer) except -1:
        cdef:
            char *p
            char *begin
            uint32_t body_map_sz
            uint32_t max_body_len
            uint32_t space_id, index_id

        space_id = self.space.sid
        index_id = self.index.iid

        body_map_sz = 2 \
                      + <uint32_t> (index_id > 0)
        # Size description:
        # mp_sizeof_map(body_map_sz)
        # + mp_sizeof_uint(TP_SPACE)
        # + mp_sizeof_uint(space)
        max_body_len = 1 \
                       + 1 \
                       + 9

        if index_id > 0:
            # mp_sizeof_uint(TP_INDEX) + mp_sizeof_uint(index)
            max_body_len += 1 + 9

        max_body_len += 1  # mp_sizeof_uint(TP_KEY);

        buffer.ensure_allocated(max_body_len)

        p = begin = &buffer._buf[buffer._length]
        p = mp_encode_map(p, body_map_sz)
        p = mp_encode_uint(p, tarantool.IPROTO_SPACE_ID)
        p = mp_encode_uint(p, space_id)

        if index_id > 0:
            p = mp_encode_uint(p, tarantool.IPROTO_INDEX_ID)
            p = mp_encode_uint(p, index_id)

        p = mp_encode_uint(p, tarantool.IPROTO_KEY)
        buffer._length += (p - begin)
        p = encode_key_sequence(buffer, p, self.key, self.index.metadata, False)
