from libc.stdint cimport uint64_t, uint32_t, int64_t


cdef class TntResponse:
    cdef:
        public uint32_t code
        public uint64_t sync
        public int64_t schema_id
        public str errmsg
        public list body
        public str encoding

    cdef inline has_schema_id(self)
    cdef inline is_error(self)
