from libc.stdint cimport int64_t


cdef class SchemaIndex:
    cdef public int sid
    cdef public int iid
    cdef public str name
    cdef public str index_type
    cdef public object unique
    cdef public list parts


cdef class SchemaSpace:
    cdef public int sid
    cdef public int arity
    cdef public str name
    cdef public dict indexes

    cdef add_index(self, SchemaIndex idx)


cdef class Schema:
    cdef:
        dict schema
        int64_t id

    cpdef get_id(self)
    cpdef SchemaSpace get_space(self, space)
    cpdef SchemaIndex get_index(self, space, index)

    cdef inline clear(self)


cdef Schema parse_schema(int64_t schema_id, spaces, indexes)
