from libc.stdint cimport uint32_t

from uuid import UUID


cdef object uuid_decode(const char ** p, uint32_t length):
    data = cpython.bytes.PyBytes_FromStringAndSize(p[0], length)
    p[0] += length
    return UUID(bytes=data)
