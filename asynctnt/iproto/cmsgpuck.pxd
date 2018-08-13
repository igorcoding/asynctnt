from libc.stdint cimport uint32_t, uint64_t, int64_t, uint8_t, uint16_t
from libc.stdio cimport FILE

cdef extern from "../../third_party/msgpuck/msgpuck.h":
    cdef enum mp_type:
        MP_NIL = 0
        MP_UINT
        MP_INT
        MP_STR
        MP_BIN
        MP_ARRAY
        MP_MAP
        MP_BOOL
        MP_FLOAT
        MP_DOUBLE
        MP_EXT

    cdef uint8_t mp_load_u8(const char **data)
    cdef uint16_t mp_load_u16(const char **data)
    cdef uint32_t mp_load_u32(const char **data)
    cdef uint64_t mp_load_u64(const char **data)

    cdef char *mp_store_u8(char *data, uint8_t val)
    cdef char *mp_store_u16(char *data, uint16_t val)
    cdef char *mp_store_u32(char *data, uint32_t val)
    cdef char *mp_store_u64(char *data, uint64_t val)

    cdef mp_type mp_typeof(const char c)

    cdef uint32_t mp_sizeof_array(uint32_t size)
    cdef char *mp_encode_array(char *data, uint32_t size)
    cdef uint32_t mp_decode_array(const char **data)

    cdef uint32_t mp_sizeof_map(uint32_t size)
    cdef char *mp_encode_map(char *data, uint32_t size)
    cdef uint32_t mp_decode_map(const char **data)

    cdef uint32_t mp_sizeof_uint(uint64_t num)
    cdef char *mp_encode_uint(char *data, uint64_t num)
    cdef uint64_t mp_decode_uint(const char **data)

    cdef uint32_t mp_sizeof_int(int64_t num)
    cdef char *mp_encode_int(char *data, int64_t num)
    cdef int64_t mp_decode_int(const char **data)

    cdef char *mp_encode_float(char *data, float num)
    cdef float mp_decode_float(const char **data)

    cdef uint32_t mp_sizeof_double(double num)
    cdef char *mp_encode_double(char *data, double num)
    cdef double mp_decode_double(const char **data)

    cdef char *mp_encode_strl(char *data, uint32_t len)
    cdef uint32_t mp_decode_strl(const char **data)

    cdef uint32_t mp_sizeof_str(uint32_t len)
    cdef char *mp_encode_str(char *data, const char *str, uint32_t len)
    cdef const char *mp_decode_str(const char **data, uint32_t *len)

    cdef char *mp_encode_binl(char *data, uint32_t len)
    cdef uint32_t mp_decode_binl(const char **data)

    cdef uint32_t mp_sizeof_bin(uint32_t len)
    cdef char *mp_encode_bin(char *data, const char *str, uint32_t len)
    cdef const char *mp_decode_bin(const char **data, uint32_t *len)

    cdef uint32_t mp_decode_strbinl(const char **data)
    cdef const char *mp_decode_strbin(const char **data, uint32_t *len)

    cdef char *mp_encode_nil(char *data)
    cdef void mp_decode_nil(const char **data)

    cdef char *mp_encode_bool(char *data, bint val)
    cdef bint mp_decode_bool(const char **data)

    cdef void mp_next(const char **data)

    cdef void mp_fprint(FILE *file, const char *data)
