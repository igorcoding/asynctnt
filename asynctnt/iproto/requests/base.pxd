from libc.stdint cimport uint32_t, uint64_t, int64_t


cdef class BaseRequest:
    cdef:
        tarantool.iproto_type op
        uint64_t sync
        int64_t schema_id
        SchemaSpace space
        object waiter
        object timeout_handle
        bint parse_metadata
        bint parse_as_tuples
        bint push_subscribe
        bint check_schema_change

    cdef inline TntFields fields(self):
        if self.space is None:
            return None
        return self.space.fields


cdef char *encode_key_sequence(WriteBuffer buffer,
                               char *p, object t,
                               TntFields fields,
                               bint default_none) except NULL