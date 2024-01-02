from cpython.datetime cimport datetime
from libc.stdint cimport int16_t, int32_t, int64_t, uint32_t


cdef struct IProtoDateTime:
    int64_t seconds
    int32_t nsec
    int16_t tzoffset
    int16_t tzindex

cdef void datetime_zero(IProtoDateTime *dt)
cdef uint32_t datetime_len(IProtoDateTime *dt)
cdef char *datetime_encode(char *p, IProtoDateTime *dt) except NULL
cdef int datetime_decode(const char ** p,
                         uint32_t length,
                         IProtoDateTime *dt) except -1
cdef void datetime_from_py(datetime ob, IProtoDateTime *dt)
cdef object datetime_to_py(IProtoDateTime *dt)
