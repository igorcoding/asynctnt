from libc.stdint cimport uint64_t, uint32_t, int64_t


cdef class Response:
    cdef:
        public uint32_t code
        public uint64_t sync
        public int64_t schema_id
        public str errmsg
        public list body
        public bytes encoding

    @staticmethod
    cdef inline Response new(bytes encoding)

    cdef inline has_schema_id(self)
    cdef inline is_error(self)


cdef Response response_parse(const char *buf, uint32_t buf_len, bytes encoding)
