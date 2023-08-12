cimport cython


@cython.final
cdef class SelectRequest(BaseRequest):
    cdef int encode_body(self, WriteBuffer buffer) except -1:
        cdef:
            char *begin
            char *p
            uint32_t body_map_sz
            uint32_t max_body_len
            uint32_t space_id, index_id

        space_id = self.space.sid
        index_id = self.index.iid

        body_map_sz = 3 \
                      + <uint32_t> (index_id > 0) \
                      + <uint32_t> (self.offset > 0) \
                      + <uint32_t> (self.iterator > 0)
        # Size description:
        # mp_sizeof_map(body_map_sz)
        # + mp_sizeof_uint(TP_SPACE)
        # + mp_sizeof_uint(space)
        # + mp_sizeof_uint(TP_LIMIT)
        # + mp_sizeof_uint(limit)
        max_body_len = 1 \
                       + 1 \
                       + 9 \
                       + 1 \
                       + 9

        if index_id > 0:
            # mp_sizeof_uint(TP_INDEX) + mp_sizeof_uint(index_id)
            max_body_len += 1 + 9
        if self.offset > 0:
            # mp_sizeof_uint(TP_OFFSET) + mp_sizeof_uint(offset)
            max_body_len += 1 + 9
        if self.iterator > 0:
            # mp_sizeof_uint(TP_ITERATOR) + mp_sizeof_uint(iterator)
            max_body_len += 1 + 1

        max_body_len += 1  # mp_sizeof_uint(TP_KEY);

        buffer.ensure_allocated(max_body_len)

        p = begin = &buffer._buf[buffer._length]
        p = mp_encode_map(p, body_map_sz)
        p = mp_encode_uint(p, tarantool.IPROTO_SPACE_ID)
        p = mp_encode_uint(p, space_id)
        p = mp_encode_uint(p, tarantool.IPROTO_LIMIT)
        p = mp_encode_uint(p, self.limit)

        if index_id > 0:
            p = mp_encode_uint(p, tarantool.IPROTO_INDEX_ID)
            p = mp_encode_uint(p, index_id)
        if self.offset > 0:
            p = mp_encode_uint(p, tarantool.IPROTO_OFFSET)
            p = mp_encode_uint(p, self.offset)
        if self.iterator > 0:
            p = mp_encode_uint(p, tarantool.IPROTO_ITERATOR)
            p = mp_encode_uint(p, self.iterator)

        p = mp_encode_uint(p, tarantool.IPROTO_KEY)
        buffer._length += (p - begin)
        p = encode_key_sequence(buffer, p, self.key, self.index.metadata, False)
