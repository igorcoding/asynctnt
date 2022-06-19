cimport cython

@cython.final
cdef class SelectRequest(BaseRequest):
    cdef inline WriteBuffer encode(self, bytes encoding):
        cdef WriteBuffer buffer = WriteBuffer.create(encoding)
        buffer.write_header(self.sync, self.op, self.schema_id)
        self.encode_request_select(buffer, self.space, self.index, self.key,
                                   self.offset, self.limit, self.iterator)
        buffer.write_length()
        return buffer

    cdef int encode_request_select(self, WriteBuffer buffer,
                                   SchemaSpace space, SchemaIndex index,
                                   key, uint64_t offset, uint64_t limit,
                                   uint32_t iterator) except -1:
        cdef:
            char *begin
            char *p
            uint32_t body_map_sz
            uint32_t max_body_len
            uint32_t space_id, index_id

        space_id = space.sid
        index_id = index.iid

        body_map_sz = 3 \
                      + <uint32_t> (index_id > 0) \
                      + <uint32_t> (offset > 0) \
                      + <uint32_t> (iterator > 0)
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
        if offset > 0:
            # mp_sizeof_uint(TP_OFFSET) + mp_sizeof_uint(offset)
            max_body_len += 1 + 9
        if iterator > 0:
            # mp_sizeof_uint(TP_ITERATOR) + mp_sizeof_uint(iterator)
            max_body_len += 1 + 1

        max_body_len += 1  # mp_sizeof_uint(TP_KEY);

        buffer.ensure_allocated(max_body_len)

        p = begin = &buffer._buf[buffer._length]
        p = mp_encode_map(p, body_map_sz)
        p = mp_encode_uint(p, tarantool.IPROTO_SPACE_ID)
        p = mp_encode_uint(p, space_id)
        p = mp_encode_uint(p, tarantool.IPROTO_LIMIT)
        p = mp_encode_uint(p, limit)

        if index_id > 0:
            p = mp_encode_uint(p, tarantool.IPROTO_INDEX_ID)
            p = mp_encode_uint(p, index_id)
        if offset > 0:
            p = mp_encode_uint(p, tarantool.IPROTO_OFFSET)
            p = mp_encode_uint(p, offset)
        if iterator > 0:
            p = mp_encode_uint(p, tarantool.IPROTO_ITERATOR)
            p = mp_encode_uint(p, iterator)

        p = mp_encode_uint(p, tarantool.IPROTO_KEY)
        buffer._length += (p - begin)
        p = encode_key_sequence(buffer, p, key, index.fields, False)
