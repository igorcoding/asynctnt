from libc.stdint cimport int64_t, uint32_t


cdef class TntField:
    cdef:
        uint32_t id
        str name
        str type

    @staticmethod
    cdef TntField new(uint32_t id, str name, str type)


cdef class SchemaIndex:
    cdef:
        int sid
        int iid
        str name
        str index_type
        object unique
        list parts

        list fields_names

    @staticmethod
    cdef SchemaIndex new()


cdef class SchemaSpace:
    cdef:
        int sid
        int owner
        str name
        str engine
        int field_count
        object flags

        dict fields_map
        list fields_names
        dict indexes

    @staticmethod
    cdef SchemaSpace new(list space_row)

    cdef add_index(self, SchemaIndex idx)


cdef class Schema:
    cdef:
        dict schema
        int64_t id

    @staticmethod
    cdef Schema new(int64_t schema_id)

    cdef SchemaSpace get_space(self, space)
    cdef SchemaIndex get_index(self, space, index)

    cdef SchemaSpace parse_space(self, list index_row)
    cdef SchemaIndex parse_index(self, list index_row)

    cdef inline clear(self)

    @staticmethod
    cdef Schema parse(int64_t schema_id, spaces, indexes)
