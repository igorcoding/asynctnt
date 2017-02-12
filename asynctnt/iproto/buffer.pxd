from libc.stdint cimport uint32_t, uint64_t, int64_t

cimport tnt


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
    cdef WriteBuffer new(bytes encoding=*)

    cdef inline _check_readonly(self)
    cdef inline len(self)
    cdef void ensure_allocated(self, ssize_t extra_length) except *
    cdef char *_ensure_allocated(self, char *p,
                                        ssize_t extra_length) except NULL
    cdef void _reallocate(self, ssize_t new_size) except *
    cdef void write_buffer(self, WriteBuffer buf) except *
    cdef void write_header(self, uint64_t sync, tnt.tp_request_type op,
                           int64_t schema_id=*) except *
    cdef void write_length(self)

    cdef char *_encode_nil(self, char *p) except NULL
    cdef char *_encode_bool(self, char *p, bint value) except NULL
    cdef char *_encode_double(self, char *p, double value) except NULL
    cdef char *_encode_uint(self, char *p, uint64_t value) except NULL
    cdef char *_encode_int(self, char *p, int64_t value) except NULL
    cdef char *_encode_str(self, char *p,
                           const char *str, uint32_t len) except NULL
    cdef char *_encode_bin(self, char *p,
                           const char *data, uint32_t len) except NULL
    cdef char *_encode_array(self, char *p, uint32_t len) except NULL
    cdef char *_encode_map(self, char *p, uint32_t len) except NULL
    cdef char *_encode_list(self, char *p, list arr) except NULL
    cdef char *_encode_tuple(self, char *p, tuple t) except NULL
    cdef char *_encode_dict(self, char *p, dict d) except NULL
    cdef char *_encode_obj(self, char *p, object o) except NULL

    cdef tnt.tnt_update_op_kind _op_type_to_kind(self, char *str, ssize_t len)
    cdef char *_encode_update_ops(self, char *p, list operations) except NULL

    cdef void encode_request_call(self, str func_name, list args) except *
    cdef void encode_request_eval(self, str expression, list args) except *
    cdef void encode_request_select(self, uint32_t space, uint32_t index,
                                    list key, uint64_t offset, uint64_t limit,
                                    uint32_t iterator) except *
    cdef void encode_request_insert(self, uint32_t space, list t) except *
    cdef void encode_request_delete(self, uint32_t space, uint32_t index,
                                    list key) except *
    cdef void encode_request_update(self, uint32_t space, uint32_t index,
                                    list key_tuple, list operations,
                                    uint32_t key_of_tuple=*,
                                    uint32_t key_of_operations=*
                                    ) except *
    cdef void encode_request_upsert(self, uint32_t space,
                                    list t, list operations) except *
    cdef void encode_request_auth(self,
                                  bytes username,
                                  bytes scramble) except *
