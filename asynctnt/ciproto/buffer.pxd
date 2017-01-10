from libc.stdint cimport uint32_t, uint64_t, int64_t
cimport tntconst

include "const.pxi"
include "cmsgpuck.pxd"

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

    cdef inline _check_readonly(self)
    cdef inline len(self)
    cdef inline ensure_allocated(self, ssize_t extra_length)
    cdef _reallocate(self, ssize_t new_size)
    cdef write_header(self, uint32_t sync, tntconst.tp_request_type op)
    cdef write_length(self)

    @staticmethod
    cdef WriteBuffer new()
