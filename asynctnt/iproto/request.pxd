from libc.stdint cimport uint32_t, uint64_t, int64_t

cimport tnt

cdef class Request:
    cdef:
        tnt.tp_request_type op
        uint64_t sync
        int64_t schema_id
        WriteBuffer buf
        object waiter
        object timeout_handle

    @staticmethod
    cdef inline Request new(tnt.tp_request_type op,
                            uint64_t sync, int64_t schema_id,
                            WriteBuffer buf)
