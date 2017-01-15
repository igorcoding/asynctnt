cimport cpython
cimport cython

from libc.string cimport memcpy
from cpython.mem cimport PyMem_Malloc, PyMem_Realloc, PyMem_Free
from cpython cimport PyBuffer_FillInfo, PyBytes_AsString
from libc.stdint cimport uint32_t, uint64_t, int64_t


cdef class Memory:
    cdef as_bytes(self):
        return cpython.PyBytes_FromStringAndSize(self.buf, self.length)

    @staticmethod
    cdef inline Memory new(char* buf, ssize_t length):
        cdef Memory mem
        mem = Memory.__new__(Memory)
        mem.buf = buf
        mem.length = length
        return mem

# noinspection PyUnresolvedReferences
# noinspection PyAttributeOutsideInit
@cython.no_gc_clear
@cython.final
@cython.freelist(_BUFFER_FREELIST_SIZE)
cdef class WriteBuffer:
    def __cinit__(self):
        self._smallbuf_inuse = True
        self._buf = self._smallbuf
        self._size = _BUFFER_INITIAL_SIZE
        self._length = 0

    def __dealloc__(self):
        if self._buf is not NULL and not self._smallbuf_inuse:
            PyMem_Free(self._buf)
            self._buf = NULL
            self._size = 0

        if self._view_count:
            raise RuntimeError(
                'Deallocating buffer with attached memoryviews')

    def __getbuffer__(self, Py_buffer *buffer, int flags):
        self._view_count += 1

        PyBuffer_FillInfo(
            buffer, self, self._buf, self._length,
            1,  # read-only
            flags)

    def __releasebuffer__(self, Py_buffer *buffer):
        self._view_count -= 1

    cdef inline _check_readonly(self):
        if self._view_count:
            raise BufferError('the buffer is in read-only mode')

    cdef inline len(self):
        return self._length

    cdef inline ensure_allocated(self, ssize_t extra_length):
        cdef ssize_t new_size = extra_length + self._length

        if new_size > self._size:
            self._reallocate(new_size)

    cdef _reallocate(self, ssize_t new_size):
        cdef char *new_buf
    
    
        print('reallocate: {}'.format(new_size))
        if new_size < _BUFFER_MAX_GROW:
            new_size = _BUFFER_MAX_GROW
        else:
            # Add a little extra
            new_size += _BUFFER_INITIAL_SIZE

        if self._smallbuf_inuse:
            new_buf = <char*>PyMem_Malloc(sizeof(char) * <size_t>new_size)
            if new_buf is NULL:
                self._buf = NULL
                self._size = 0
                self._length = 0
                raise MemoryError
            memcpy(new_buf, self._buf, <size_t>self._size)
            self._size = new_size
            self._buf = new_buf
            self._smallbuf_inuse = False
        else:
            new_buf = <char*>PyMem_Realloc(<void*>self._buf, <size_t>new_size)
            if new_buf is NULL:
                PyMem_Free(self._buf)
                self._buf = NULL
                self._size = 0
                self._length = 0
                raise MemoryError
            self._buf = new_buf
            self._size = new_size

    @staticmethod
    cdef WriteBuffer new():
        cdef WriteBuffer buf
        buf = WriteBuffer.__new__(WriteBuffer)
        return buf
    
    cdef write_header(self, uint64_t sync, tnt.tp_request_type op):
        cdef char* p = NULL
        self.ensure_allocated(HEADER_CONST_LEN)
        
        p = &self._buf[self._length]
        p = mp_encode_map(&p[5], 2)
        p = mp_encode_uint(p, tnt.TP_CODE)
        p = mp_encode_uint(p, <uint32_t>op)
        p = mp_encode_uint(p, tnt.TP_SYNC)
        p = mp_encode_uint(p, sync)
        self._length += (p - self._buf)
        
    cdef write_length(self):
        cdef:
            char* p
        p = self._buf
        p = mp_store_u8(p, 0xce)
        p = mp_store_u32(p, self._length - 5)



cdef class ReadBuffer:
    cdef:
        char* buf
