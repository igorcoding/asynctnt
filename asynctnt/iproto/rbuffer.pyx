cimport cpython
cimport cython
cimport cpython.bytes
cimport cpython.list
cimport cpython.tuple
cimport cpython.dict
cimport cpython.unicode

from cpython.mem cimport PyMem_Malloc, PyMem_Realloc, PyMem_Free
from cpython.ref cimport PyObject

from libc.string cimport memcpy, memmove
from libc.stdint cimport uint32_t, uint64_t, int64_t

from asynctnt.exceptions import TarantoolRequestError
from asynctnt.log import logger


@cython.no_gc_clear
@cython.final
@cython.freelist(_BUFFER_FREELIST_SIZE)
cdef class ReadBuffer:
    def __cinit__(self):
        self.buf = NULL
        self.len = 0
        self.use = 0
        self.encoding = None

    @staticmethod
    cdef ReadBuffer new(str encoding, size_t initial_buffer_size=0x80000):
        cdef ReadBuffer b
        b = ReadBuffer.__new__(ReadBuffer)

        b.buf = <char*>PyMem_Malloc(sizeof(char) * <size_t>initial_buffer_size)
        if b.buf is NULL:
            raise MemoryError

        b.len = initial_buffer_size
        b.use = 0
        b.encoding = encoding
        return b

    def __dealloc__(self):
        if self.buf is not NULL:
            PyMem_Free(self.buf)
            self.buf = NULL
        self.len = 0
        self.use = 0

    cdef void _reallocate(self, size_t new_size) except *:
        cdef char *new_buf

        # logger.debug('reallocate: {}'.format(new_size))
        new_buf = <char*>PyMem_Realloc(<void*>self.buf, <size_t>new_size)
        if new_buf is NULL:
            PyMem_Free(self.buf)
            self.buf = NULL
            self.len = 0
            self.use = 0
            raise MemoryError
        self.buf = new_buf
        self.len = new_size

    cdef int extend(self, const char *data, size_t len) except -1:
        cdef size_t new_size
        new_size = self.use + len
        if new_size > self.len:
            self._reallocate(self.len * 2)
            # raise OverflowError('buffer limit {} exceeded'.format(self.len))

        memcpy(&self.buf[self.use], data, len)
        self.use += len
        return 0

    cdef void move(self, size_t pos):
        cdef size_t delta = self.use - pos
        memmove(self.buf, &self.buf[pos], delta)
        self.use = delta

    cdef void move_ptr(self, const char *src, size_t size):
        memmove(self.buf, src, size)

    cdef bytes get_slice(self, size_t begin, size_t end):
        cdef:
            ssize_t diff
            char *p
        p = &self.buf[begin]
        diff = end - begin
        return <bytes>p[:diff]

    cdef bytes get_slice_begin(self, size_t begin):
        cdef:
            ssize_t diff
            char *p
        p = &self.buf[begin]
        diff = self.use - begin
        return <bytes>p[:diff]

    cdef bytes get_slice_end(self, size_t end):
        cdef:
            ssize_t diff
            char *p
        p = &self.buf[0]
        diff = end - 0
        return <bytes>p[:diff]
