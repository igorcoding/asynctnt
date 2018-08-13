from libc.stdint cimport uint64_t, int64_t


cdef public class TntFields [object C_TntFields, type C_TntFields_Type]:
    cdef:
        list _names  # contains only field names ('f1', 'f2', ...)
        dict _mapping  # contains field's name => id mappings

    cdef inline int len(self)
    cdef inline void add(self, uint64_t id, str name)
    cdef inline str name_by_id(self, int i)
    cdef inline uint64_t id_by_name(self, str f) except *


cdef class SchemaIndex:
    cdef:
        int sid
        int iid
        str name
        str index_type
        object unique

        TntFields fields


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

        TntFields fields
        dict indexes

    cdef void add_index(self, SchemaIndex idx)
    cdef SchemaIndex get_index(self, index, create_dummy=*)


cdef class SchemaDummySpace(SchemaSpace):
    pass


cdef class Schema:
    cdef:
        dict schema
        int64_t id

    cdef SchemaSpace get_space(self, space)
    cdef SchemaSpace create_dummy_space(self, int space_id)
    cdef SchemaSpace get_or_create_space(self, space)

    cdef SchemaSpace parse_space(self, space_row)
    cdef SchemaIndex parse_index(self, index_row)

    cdef inline clear(self)

    @staticmethod
    cdef Schema parse(int64_t schema_id, spaces, indexes)


cdef list dict_to_list_fields(dict d, TntFields fields, bint default_none)
