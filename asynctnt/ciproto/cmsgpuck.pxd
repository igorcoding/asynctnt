from libc.stdint cimport uint32_t, uint64_t, int64_t

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
    
    cdef mp_type mp_typeof(const char c);
    
    cdef uint32_t mp_decode_array(const char **data);
    
    cdef uint32_t mp_decode_map(const char **data);
    
    cdef uint64_t mp_decode_uint(const char **data);
    
    cdef int64_t mp_decode_int(const char **data);
    
    cdef float mp_decode_float(const char **data);
    
    cdef double mp_decode_double(const char **data);
    
    cdef uint32_t mp_decode_strl(const char **data);
    cdef const char *mp_decode_str(const char **data, uint32_t *len);
    
    cdef uint32_t mp_decode_binl(const char **data);
    cdef const char *mp_decode_bin(const char **data, uint32_t *len);
    
    cdef uint32_t mp_decode_strbinl(const char **data);
    cdef const char *mp_decode_strbin(const char **data, uint32_t *len);
    
    cdef void mp_decode_nil(const char **data);
    cdef bint mp_decode_bool(const char **data);
    
    cdef void mp_next(const char **data);
    
