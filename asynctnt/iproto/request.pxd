from libc.stdint cimport uint32_t, uint64_t, int64_t

cimport tnt

cdef class Request:
    cdef:
        tnt.tp_request_type op
        uint64_t sync
        WriteBuffer buf
        object waiter
        object timeout_handle

    @staticmethod
    cdef inline Request new(tnt.tp_request_type op,
                            uint64_t sync,
                            WriteBuffer buf)


cdef Request request_ping(bytes encoding, uint64_t sync)

cdef Request request_call(bytes encoding, uint64_t sync,
                          str func_name, list args)

cdef Request request_call16(bytes encoding, uint64_t sync,
                            str func_name, list args)

cdef Request request_eval(bytes encoding, uint64_t sync,
                          str expression, list args)

cdef Request request_select(bytes encoding, uint64_t sync,
                            uint32_t space, uint32_t index, list key,
                            uint64_t offset, uint64_t limit, uint32_t iterator)

cdef Request request_insert(bytes encoding, uint64_t sync,
                            uint32_t space, list t, bint replace)

cdef Request request_delete(bytes encoding, uint64_t sync,
                            uint32_t space, uint32_t index, list key)

cdef Request request_update(bytes encoding, uint64_t sync,
                            uint32_t space, uint32_t index,
                            list key, list operations)

cdef Request request_upsert(bytes encoding, uint64_t sync,
                            uint32_t space,
                            list t, list operations)

cdef Request request_auth(bytes encoding, uint64_t sync,
                          bytes salt, str username, str password)
