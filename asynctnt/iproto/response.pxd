from libc.stdint cimport uint64_t, uint32_t, int64_t


cdef class TntResponse:
    cdef:
        public uint32_t code
        public uint64_t sync
        public int64_t schema_id
        public str errmsg
        public list body
        public bytes encoding

    @staticmethod
    cdef inline TntResponse new(bytes encoding)

    cdef inline has_schema_id(self)
    cdef inline is_error(self)


cdef TntResponse response_parse(const char *buf, uint32_t buf_len, bytes encoding=*)
