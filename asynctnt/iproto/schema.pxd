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

        list fields

    @staticmethod
    cdef SchemaIndex new()


cdef class SchemaDummyIndex(SchemaIndex):
    pass


cdef class SchemaSpace:
    cdef:
        int sid
        int owner
        str name
        str engine
        int field_count
        object flags

        list fields
        dict fields_map
        dict indexes

    @staticmethod
    cdef SchemaSpace new()

    cdef add_index(self, SchemaIndex idx)
    cdef SchemaIndex get_index(self, index, create_dummy=*)


cdef class SchemaDummySpace(SchemaSpace):
    pass


cdef class Schema:
    cdef:
        dict schema
        int64_t id

    @staticmethod
    cdef Schema new(int64_t schema_id)

    cdef SchemaSpace get_space(self, space)
    cdef SchemaSpace create_dummy_space(self, int space_id)
    cdef SchemaSpace get_or_create_space(self, space)

    cdef SchemaSpace parse_space(self, list index_row)
    cdef SchemaIndex parse_index(self, list index_row)

    cdef inline clear(self)

    @staticmethod
    cdef Schema parse(int64_t schema_id, spaces, indexes)


cdef list dict_to_list_fields(list fields, dict d, bint default_none)
