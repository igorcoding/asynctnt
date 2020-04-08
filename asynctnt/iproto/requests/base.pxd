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

    cdef inline TntFields fields(self):
        if self.space is None:
            return None
        return self.space.fields
