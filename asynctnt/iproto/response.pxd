from libc.stdint cimport uint64_t, uint32_t, int64_t


cdef class Response:
    cdef:
        uint32_t _code
        uint64_t _sync
        int64_t _schema_id
        str _errmsg
        list _body
        bytes _encoding

    @staticmethod
    cdef inline Response new(bytes encoding)

    cdef inline is_error(self)


cdef Response response_parse(const char *buf, uint32_t buf_len, bytes encoding)
