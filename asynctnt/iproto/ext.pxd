from libc.stdint cimport uint32_t, uint8_t

cdef uint32_t decimal_len(int exponent, tuple digits)
cdef char *decimal_encode(char *p, uint8_t sign, tuple digits, int exponent)
cdef object decimal_decode(const char **p, uint32_t length)
