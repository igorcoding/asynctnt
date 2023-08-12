from libc.stdint cimport uint16_t, uint32_t, uint64_t
from libc.string cimport memcpy


cdef inline uint64_t load_u64(const void * p):
    cdef:
        uint64_t res

    res = 0
    memcpy(&res, p, sizeof(res))
    return res

cdef inline uint64_t load_u32(const void * p):
    cdef:
        uint32_t res

    res = 0
    memcpy(&res, p, sizeof(res))
    return res

cdef inline uint64_t load_u16(const void * p):
    cdef:
        uint16_t res

    res = 0
    memcpy(&res, p, sizeof(res))
    return res

cdef inline void store_u64(void * p, uint64_t v):
    memcpy(p, &v, sizeof(v))

cdef inline void store_u32(void * p, uint32_t v):
    memcpy(p, &v, sizeof(v))

cdef inline void store_u16(void * p, uint16_t v):
    memcpy(p, &v, sizeof(v))
