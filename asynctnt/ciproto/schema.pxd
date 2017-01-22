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

    cpdef SchemaSpace get_space(self, space)
    cpdef SchemaIndex get_index(self, space, index)

    cdef inline clear(self)
    
    
cdef Schema parse_schema(spaces, indexes)
