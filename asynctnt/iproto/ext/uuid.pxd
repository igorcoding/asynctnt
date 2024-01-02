from libc.stdint cimport uint32_t


cdef object uuid_decode(const char ** p, uint32_t length)
