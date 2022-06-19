cimport cython

@cython.final
cdef class InsertRequest(BaseRequest):
    
    cdef inline WriteBuffer encode(self, bytes encoding):
        cdef WriteBuffer buffer = WriteBuffer.create(encoding)
        buffer.write_header(self.sync, self.op, self.schema_id)
        self.encode_request_insert(buffer, self.space, self.t)
        buffer.write_length()
        return buffer

    cdef int encode_request_insert(self, WriteBuffer buffer, SchemaSpace space, t) except -1:
        cdef:
            char *begin
            char *p
            uint32_t body_map_sz
            uint32_t max_body_len
            uint32_t space_id

        space_id = space.sid

        body_map_sz = 2
        # Size description:
        # mp_sizeof_map(body_map_sz)
        # + mp_sizeof_uint(TP_SPACE)
        # + mp_sizeof_uint(space)
        # + mp_sizeof_uint(TP_TUPLE)
        max_body_len = 1 \
                       + 1 \
                       + 9 \
                       + 1

        buffer.ensure_allocated(max_body_len)

        p = begin = &buffer._buf[buffer._length]
        p = mp_encode_map(p, body_map_sz)
        p = mp_encode_uint(p, tarantool.IPROTO_SPACE_ID)
        p = mp_encode_uint(p, space_id)

        p = mp_encode_uint(p, tarantool.IPROTO_TUPLE)
        buffer._length += (p - begin)
        p = encode_key_sequence(buffer, p, t, space.fields, True)
