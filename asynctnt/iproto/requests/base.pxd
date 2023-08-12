from libc.stdint cimport int64_t, uint64_t


cdef class BaseRequest:
    cdef:
        tarantool.iproto_type op
        uint64_t sync
        int64_t schema_id
        uint64_t stream_id
        SchemaSpace space
        object waiter
        object timeout_handle
        bint parse_metadata
        bint parse_as_tuples
        bint push_subscribe
        bint check_schema_change

    cdef inline Metadata metadata(self):
        if self.space is None:
            return None
        return self.space.metadata

    cdef inline WriteBuffer encode(self, bytes encoding)
    cdef int encode_body(self, WriteBuffer buffer) except -1


cdef char *encode_key_sequence(WriteBuffer buffer,
                               char *p, object t,
                               Metadata metadata,
                               bint default_none) except NULL
