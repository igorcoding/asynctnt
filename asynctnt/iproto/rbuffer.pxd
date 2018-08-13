from libc.stdint cimport uint32_t


cdef inline size_t size_t_max(size_t a, size_t b):
    if a > b:
        return a
    return b


cdef inline uint32_t nearest_power_of_2(uint32_t v):
    v -= 1
    v |= v >> 1
    v |= v >> 2
    v |= v >> 4
    v |= v >> 8
    v |= v >> 16
    v += 1
    return v


cdef class ReadBuffer:
    cdef:
        char *buf
        size_t initial_buffer_size  # Initial buffer size, obviously
        size_t len  # Allocated size
        size_t use  # Used size

        str encoding

    @staticmethod
    cdef ReadBuffer new(str encoding, size_t initial_buffer_size=*)

    cdef void _reallocate(self, size_t new_size) except *
    cdef int extend(self, const char *data, size_t len) except -1
    cdef void move(self, size_t pos)
    cdef void move_offset(self, ssize_t offset, size_t size) except *
    cdef bytes get_slice(self, size_t begin, size_t end)
    cdef bytes get_slice_begin(self, size_t begin)
    cdef bytes get_slice_end(self, size_t end)
