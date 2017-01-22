from libc.stdint cimport uint32_t, uint64_t, int64_t

cimport tnt

cdef class Memory:
    cdef:
        char* buf
        ssize_t length

    cdef as_bytes(self)

    @staticmethod
    cdef inline Memory new(char* buf, ssize_t length)
    
    
cdef class WriteBuffer:
    cdef:
        # Preallocated small buffer
        bint _smallbuf_inuse
        char _smallbuf[_BUFFER_INITIAL_SIZE]
        
        char *_buf
        ssize_t _size  # Allocated size
        ssize_t _length  # Length of data in the buffer
        int _view_count  # Number of memoryviews attached to the buffer
        
        str _encoding

    cdef inline _check_readonly(self)
    cdef inline len(self)
    cdef inline void ensure_allocated(self, ssize_t extra_length)
    cdef void _reallocate(self, ssize_t new_size)
    cdef void write_header(self, uint64_t sync, tnt.tp_request_type op)
    cdef void write_length(self)
    
    cdef char* _encode_nil(self, char* p)
    cdef char* _encode_bool(self, char* p, bint value)
    cdef char* _encode_double(self, char* p, double value)
    cdef char* _encode_uint(self, char* p, uint64_t value)
    cdef char* _encode_int(self, char* p, int64_t value)
    cdef char* _encode_str(self, char* p, const char* str, uint32_t len)
    cdef char* _encode_bin(self, char* p, const char* data, uint32_t len)
    cdef char* _encode_array(self, char* p, uint32_t len)
    cdef char* _encode_map(self, char* p, uint32_t len)
    cdef char* _encode_list(self, char* p, list arr)
    cdef char* _encode_dict(self, char* p, dict d)
    cdef char* _encode_obj(self, char* p, object o)
    
    cdef void encode_request_call(self, str func_name, list args)
    cdef void encode_request_eval(self, str expression, list args)
    cdef void encode_request_select(self, uint32_t space, uint32_t index,
                                    list key, uint64_t offset, uint64_t limit,
                                    uint32_t iterator)
    

    @staticmethod
    cdef WriteBuffer new(str encoding)
