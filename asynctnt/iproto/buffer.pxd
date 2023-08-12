cimport cython
from libc.stdint cimport int64_t, uint32_t, uint64_t


@cython.final
cdef class WriteBuffer:
    cdef:
        # Preallocated small buffer
        bint _smallbuf_inuse
        char _smallbuf[_BUFFER_INITIAL_SIZE]

        char *_buf
        ssize_t _size  # Allocated size
        ssize_t _length  # Length of data in the buffer
        int _view_count  # Number of memoryviews attached to the buffer

        bytes _encoding

    @staticmethod
    cdef WriteBuffer create(bytes encoding)

    cdef inline _check_readonly(self)
    cdef inline len(self)
    cdef int ensure_allocated(self, ssize_t extra_length) except -1
    cdef char *_ensure_allocated(self, char *p,
                                 ssize_t extra_length) except NULL
    cdef int _reallocate(self, ssize_t new_size) except -1
    cdef int write_buffer(self, WriteBuffer buf) except -1
    cdef int write_header(self, uint64_t sync,
                          tarantool.iproto_type op,
                          int64_t schema_id,
                          uint64_t stream_id) except -1
    cdef void write_length(self)

    cdef char *mp_encode_nil(self, char *p) except NULL
    cdef char *mp_encode_bool(self, char *p, bint value) except NULL
    cdef char *mp_encode_double(self, char *p, double value) except NULL
    cdef char *mp_encode_uint(self, char *p, uint64_t value) except NULL
    cdef char *mp_encode_int(self, char *p, int64_t value) except NULL
    cdef char *mp_encode_str(self, char *p,
                             const char *str, uint32_t len) except NULL
    cdef char *mp_encode_bin(self, char *p,
                             const char *data, uint32_t len) except NULL
    cdef char *mp_encode_decimal(self, char *p, object value) except NULL
    cdef char *mp_encode_uuid(self, char *p, object value) except NULL
    cdef char *mp_encode_datetime(self, char *p, object value) except NULL
    cdef char *mp_encode_array(self, char *p, uint32_t len) except NULL
    cdef char *mp_encode_map(self, char *p, uint32_t len) except NULL
    cdef char *mp_encode_list(self, char *p, list arr) except NULL
    cdef char *mp_encode_tuple(self, char *p, tuple t) except NULL
    cdef char *mp_encode_dict(self, char *p, dict d) except NULL
    cdef char *mp_encode_obj(self, char *p, object o) except NULL
