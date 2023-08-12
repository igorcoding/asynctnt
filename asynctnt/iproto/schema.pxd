from libc.stdint cimport int64_t


cdef class Field:
    cdef:
        readonly str name
        readonly str type
        readonly str collation
        readonly object is_nullable
        readonly object is_autoincrement
        readonly str span


cdef public class Metadata [object C_Metadata, type C_Metadata_Type]:
    cdef:
        readonly list fields
        readonly dict name_id_map
        list names

    cdef inline int len(self)
    cdef inline void add(self, int id, Field field)
    cdef inline str name_by_id(self, int i)
    cdef inline int id_by_name(self, str name) except *


cdef class SchemaIndex:
    cdef:
        readonly int sid
        readonly int iid
        readonly str name
        readonly str index_type
        readonly object unique
        readonly Metadata metadata


cdef class SchemaDummyIndex(SchemaIndex):
    pass


cdef class SchemaSpace:
    cdef:
        readonly int sid
        readonly int owner
        readonly str name
        readonly str engine
        readonly int field_count
        readonly object flags

        readonly Metadata metadata
        readonly dict indexes

    cdef void add_index(self, SchemaIndex idx)
    cdef SchemaIndex get_index(self, index, create_dummy=*)


cdef class SchemaDummySpace(SchemaSpace):
    pass


cdef class Schema:
    cdef:
        readonly dict spaces
        readonly int id

    cdef SchemaSpace get_space(self, space)
    cdef SchemaSpace create_dummy_space(self, int space_id)
    cdef SchemaSpace get_or_create_space(self, space)

    cdef SchemaSpace parse_space(self, space_row)
    cdef SchemaIndex parse_index(self, index_row)

    cdef inline clear(self)

    @staticmethod
    cdef Schema parse(int64_t schema_id, spaces, indexes)


cdef list dict_to_list_fields(dict d, Metadata metadata, bint default_none)
