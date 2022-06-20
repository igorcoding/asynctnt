from libc.stdint cimport uint32_t, uint8_t
from libc cimport math

cdef inline uint32_t bcd_len(uint32_t digits_len):
    return <uint32_t> math.floor(digits_len / 2) + 1

cdef uint32_t decimal_len(int exponent, uint32_t digits_count)
cdef char *decimal_encode(char *p, uint32_t digits_count, uint8_t sign, tuple digits, int exponent)
cdef object decimal_decode(const char **p, uint32_t length)

cdef object uuid_decode(const char **p, uint32_t length)
