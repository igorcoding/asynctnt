from libc.stdint cimport uint32_t, uint64_t, int64_t


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

        ssize_t __op_offset
        ssize_t __sync_offset
        ssize_t __schema_id_offset

    @staticmethod
    cdef WriteBuffer new(bytes encoding)

    cdef inline _check_readonly(self)
    cdef inline len(self)
    cdef void ensure_allocated(self, ssize_t extra_length) except *
    cdef char *_ensure_allocated(self, char *p,
                                        ssize_t extra_length) except NULL
    cdef void _reallocate(self, ssize_t new_size) except *
    cdef void write_buffer(self, WriteBuffer buf) except *
    cdef void write_header(self, uint64_t sync,
                           tarantool.iproto_type op,
                           int64_t schema_id) except *
    cdef void change_schema_id(self, int64_t new_schema_id)
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
    cdef char *_encode_key_sequence(self, char *p, t,
                                    TntFields fields=*,
                                    bint default_none=*) except NULL
    cdef char *_encode_obj(self, char *p, object o) except NULL
    cdef char *_encode_update_ops(self, char *p, list operations,
                                  SchemaSpace space) except NULL

    cdef void encode_request_call(self, str func_name, args) except *
    cdef void encode_request_eval(self, str expression, args) except *
    cdef void encode_request_select(self, SchemaSpace space, SchemaIndex index,
                                    key, uint64_t offset, uint64_t limit,
                                    uint32_t iterator) except *
    cdef void encode_request_insert(self, SchemaSpace space, t) except *
    cdef void encode_request_delete(self, SchemaSpace space, SchemaIndex index,
                                    key) except *
    cdef void encode_request_update(self, SchemaSpace space, SchemaIndex index,
                                    key_tuple, list operations,
                                    bint is_upsert=*) except *
    cdef void encode_request_upsert(self, SchemaSpace space,
                                    t, list operations) except *
    cdef void encode_request_sql(self, str query, args) except *
    cdef void encode_request_auth(self,
                                  bytes username,
                                  bytes scramble) except *
