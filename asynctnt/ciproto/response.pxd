from libc.stdint cimport uint64_t

cdef class TntResponse:
    cdef:
        int code
        uint64_t sync
        int schema_id
        str errmsg
        list body
        
    cdef inline has_schema_id(self):
        return self.schema_id != -1
