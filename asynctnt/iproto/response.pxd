from libc.stdint cimport uint64_t, uint32_t


cdef class TntResponse:
    cdef:
        public uint32_t code
        public uint64_t sync
        public uint64_t schema_id
        public str errmsg
        public list body

    cdef inline has_schema_id(self)
    cdef inline is_error(self)
