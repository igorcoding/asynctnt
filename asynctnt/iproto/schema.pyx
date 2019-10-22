# cython: profile=True

from asynctnt.exceptions import TarantoolSchemaError
from asynctnt.log import logger

from libc.stdint cimport int32_t, uint64_t
cimport cpython.list
cimport cython
from cpython.ref cimport PyObject


@cython.final
@cython.freelist(TNT_FIELDS_FREELIST_SIZE)
cdef public class TntFields [object C_TntFields, type C_TntFields_Type]:
    def __cinit__(self):
        self._names = []
        self._mapping = {}

    cdef inline void add(self, uint64_t id, str name):
        self._names.append(name)
        self._mapping[name] = id

    cdef inline str name_by_id(self, int id):
        return <str>cpython.list.PyList_GetItem(self._names, id)

    cdef inline uint64_t id_by_name(self, str f) except *:
        cdef:
            PyObject *fld

        fld = cpython.dict.PyDict_GetItem(self._mapping, f)
        if fld == NULL:
            raise KeyError('Field \'{}\' not found'.format(f))
        return <uint64_t><object>fld

    cdef inline int len(self):
        return <int>cpython.list.PyList_GET_SIZE(self._names)

    def __repr__(self):
        return '<TntFields [{}]>'.format(', '.join(self._names))


cdef class SchemaIndex:
    def __cinit__(self):
        self.iid = -1
        self.sid = -1
        self.name = None
        self.index_type = None
        self.unique = None
        self.fields = None

    def __repr__(self):
        return \
            '<{} sid={}, id={}, name={}, ' \
            'type={}, unique={}>'.format(
                self.__class__.__name__,
                self.sid, self.iid, self.name, self.index_type, self.unique
            )


cdef class SchemaDummyIndex(SchemaIndex):
    pass


cdef class SchemaSpace:
    def __cinit__(self):
        self.sid = -1
        self.owner = -1
        self.name = None
        self.engine = None
        self.field_count = 0
        self.flags = None

        self.fields = None
        self.indexes = {}

    cdef void add_index(self, SchemaIndex idx):
        cpython.dict.PyDict_SetItem(self.indexes, idx.iid, idx)
        if idx.name:
            cpython.dict.PyDict_SetItem(self.indexes, idx.name, idx)

    cdef SchemaIndex get_index(self, index, create_dummy=True):
        cdef:
            SchemaIndex idx
            bint is_str, is_int
            PyObject *obj_p
        is_str = isinstance(index, str)
        is_int = isinstance(index, int)
        if not is_str and not is_int:
            raise TypeError(
                'Index must be either str or int, got: {}'.format(type(index)))

        obj_p = cpython.dict.PyDict_GetItem(self.indexes, index)
        if obj_p is not NULL:
            return <SchemaIndex>obj_p
        else:
            if is_int and create_dummy:
                logger.debug(
                    'Index %s not found in space %s/%s. Creating dummy.',
                    index, self.sid, self.name
                )
                idx = SchemaDummyIndex.__new__(SchemaDummyIndex)
                idx.iid = index
                idx.sid = self.sid
                idx.name = str(index)
                cpython.dict.PyDict_SetItem(self.indexes, index, idx)
                return idx
            else:
                raise TarantoolSchemaError(
                    'Index {} not found in space {}/{}'.format(
                        index, self.sid, self.name
                    )
                )

    def __repr__(self):
        return '<{} id={} name={} engine={}>'.format(
            self.__class__.__name__,
            self.sid, self.name, self.engine
        )


cdef class SchemaDummySpace(SchemaSpace):
    pass


cdef class Schema:
    def __cinit__(self, int64_t schema_id):
        self.id = schema_id
        self.schema = {}

    cdef SchemaSpace get_space(self, space):
        cdef PyObject *obj_p = \
            cpython.dict.PyDict_GetItem(self.schema, space)
        if obj_p is NULL:
            return None
        return <SchemaSpace>obj_p

    cdef SchemaSpace get_or_create_space(self, space):
        cdef:
            bint is_str, is_int
            PyObject *obj_p
        is_str = isinstance(space, str)
        is_int = isinstance(space, int)
        if not is_str and not is_int:
            raise TypeError(
                'Space must be either str or int, got: {}'.format(type(space)))

        obj_p = cpython.dict.PyDict_GetItem(self.schema, space)
        if obj_p is NULL:
            if is_str:
                raise TarantoolSchemaError(
                    'Space {} not found'.format(space)
                )
            else:
                return self.create_dummy_space(space)
        return <SchemaSpace>obj_p

    cdef SchemaSpace create_dummy_space(self, int space_id):
        cdef SchemaSpace s
        logger.debug('Space %s not found. Creating dummy.', space_id)
        s = SchemaDummySpace.__new__(SchemaDummySpace)
        s.sid = space_id
        s.name = str(space_id)
        cpython.dict.PyDict_SetItem(self.schema, space_id, s)
        return s

    cdef inline clear(self):
        self.schema.clear()

    cdef SchemaSpace parse_space(self, space_row):
        cdef:
            SchemaSpace sp
            size_t k
            size_t row_len
            list format_list

        assert space_row is not None

        sp = SchemaSpace.__new__(SchemaSpace)
        row_len = <size_t>len(space_row)

        k = 0
        sp.sid = <int>space_row[k]
        k += 1
        if k < row_len:
            sp.owner = <int>space_row[k]
        k += 1
        if k < row_len:
            sp.name = <str>space_row[k]
        k += 1
        if k < row_len:
            sp.engine = <str>space_row[k]
        k += 1
        if k < row_len:
            sp.field_count = <int>space_row[k]
        k += 1
        if k < row_len:
            sp.flags = space_row[k]
        k += 1
        if k < row_len:
            # format
            format_list = <list>space_row[k]
            if len(format_list) > 0:
                sp.fields = TntFields.__new__(TntFields)
                for i in range(len(format_list)):
                    field_id = i
                    field_name = format_list[i]['name']
                    # field_type = format_list[i]['type']

                    sp.fields.add(field_id, field_name)

        return sp

    cdef SchemaIndex parse_index(self, index_row):
        cdef:
            SchemaIndex idx
            SchemaSpace sp
            uint32_t i
            int field_id = -1
            str field_type

        assert index_row is not None

        idx = SchemaIndex.__new__(SchemaIndex)
        idx.sid = <int>index_row[0]
        idx.iid = <int>index_row[1]
        idx.name = <str>index_row[2]
        idx.index_type = <str>index_row[3]
        idx.unique = <object>index_row[4]
        idx.fields = None

        sp = self.get_space(idx.sid)
        if sp is None:
            raise TarantoolSchemaError(
                'Space with id {} not found'.format(idx.sid))

        if sp.fields is not None:
            parts = index_row[5]
            if isinstance(parts, (list, tuple)):
                idx.fields = TntFields.__new__(TntFields)
                for part in parts:
                    if isinstance(part, (list, tuple)):
                        assert len(part) == 2, 'Part len must be 2'
                        field_id = part[0]
                        field_type = part[1]
                    elif isinstance(part, dict):
                        field_id = part['field']
                        field_type = part['type']
                        # TODO: add is_nullable and collation if we really need
                        # TODO: it in a python driver

                    if field_id < sp.fields.len():
                        f = sp.fields.name_by_id(field_id)
                        idx.fields.add(field_id, f)
                    else:
                        logger.debug(
                            'Field #%d of space %s is not '
                            'in space format definition', field_id, sp.name)
                        idx.fields.add(field_id, '')
            else:
                idx.fields = TntFields.__new__(TntFields)
                for i in range(index_row[5]):
                    field_id = index_row[5 + 1 + i * 2]
                    field_type = index_row[5 + 2 + i * 2]

                    if field_id < sp.fields.len():
                        f = sp.fields.name_by_id(field_id)
                        idx.fields.add(field_id, f)
                    else:
                        logger.debug(
                            'Field #%d of space %s is not '
                            'in space format definition', field_id, sp.name)
                        idx.fields.add(field_id, '')

        return idx

    @staticmethod
    cdef Schema parse(int64_t schema_id, spaces, indexes):
        cdef:
            Schema s
            SchemaSpace sp
            SchemaIndex idx

        s = Schema.__new__(Schema, schema_id)
        for space_row in spaces:
            sp = s.parse_space(space_row)
            cpython.dict.PyDict_SetItem(s.schema, sp.sid, sp)
            if sp.name:
                cpython.dict.PyDict_SetItem(s.schema, sp.name, sp)

        for index_row in indexes:
            idx = s.parse_index(index_row)
            sp = s.schema[idx.sid]
            sp.add_index(idx)

        return s

    def __repr__(self):
        return '<Schema spaces={}>'.format(len(self.schema))


cdef list dict_to_list_fields(dict d, TntFields fields, bint default_none):
    cdef:
        list tpl
        dict used
        object value
        str field_name
        int32_t used_count
        int field_id
        PyObject *obj_p

    assert fields is not None
    assert d is not None

    tpl = []
    used_count = <int32_t>cpython.dict.PyDict_Size(d)

    for field_id in range(fields.len()):
        field_name = fields.name_by_id(field_id)

        obj_p = cpython.dict.PyDict_GetItem(d, field_name)
        if obj_p is not NULL:
            value = <object>obj_p
            used_count -= 1
            tpl.append(value)
        else:
            if default_none:
                tpl.append(None)

    # Warn user if he used any of unknown fields
    if used_count != 0:
        used = {}
        for field_id in range(fields.len()):
            field_name = fields.name_by_id(field_id)

            if <bint>cpython.dict.PyDict_Contains(d, field_name):
                used[field_name] = None
        if <bint>cpython.dict.PyDict_Contains(d, ''):
            used[''] = None

        for f in d:
            if f not in used:
                logger.warning(
                    'Field \'%s\' in supplied dict is unknown as '
                    'a tuple field for selected index. Skipping.',
                    f
                )
    return tpl
