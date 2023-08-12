from cpython.datetime cimport datetime
from libc cimport math
from libc.stdint cimport int16_t, int32_t, int64_t, uint8_t, uint32_t


cdef inline uint32_t bcd_len(uint32_t digits_len):
    return <uint32_t> math.floor(digits_len / 2) + 1

cdef uint32_t decimal_len(int exponent, uint32_t digits_count)
cdef char *decimal_encode(char *p,
                          uint32_t digits_count,
                          uint8_t sign,
                          tuple digits,
                          int exponent) except NULL
cdef object decimal_decode(const char ** p, uint32_t length)

cdef object uuid_decode(const char ** p, uint32_t length)

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
