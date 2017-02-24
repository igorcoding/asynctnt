from asynctnt.exceptions import TarantoolSchemaError
from asynctnt.log import logger
from cpython.mem cimport PyMem_Malloc, PyMem_Free
from libc.stdint cimport int32_t

cdef class TntField:
    @staticmethod
    cdef TntField new(uint32_t id, str name, str type):
        cdef TntField f
        f = TntField.__new__(TntField)
        f.id = id
        f.name = name
        f.type = type
        return f


cdef class SchemaIndex:
    @staticmethod
    cdef SchemaIndex new():
        cdef SchemaIndex idx
        idx = SchemaIndex.__new__(SchemaIndex)
        idx.iid = -1
        idx.sid = -1
        idx.name = None
        idx.index_type = None
        idx.unique = None
        idx.parts = []
        idx.fields = []
        return idx

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

    @staticmethod
    cdef SchemaSpace new():
        cdef SchemaSpace sp
        sp = SchemaSpace.__new__(SchemaSpace)
        sp.sid = -1
        sp.owner = -1
        sp.name = None
        sp.engine = None
        sp.field_count = 0
        sp.flags = None

        sp.fields_map = {}
        sp.fields = []
        sp.indexes = {}
        return sp

    cdef add_index(self, SchemaIndex idx):
        self.indexes[idx.iid] = idx
        if idx.name:
            self.indexes[idx.name] = idx

    cdef SchemaIndex get_index(self, index, create_dummy=True):
        cdef:
            SchemaIndex idx
            bint is_str, is_int
        is_str = isinstance(index, str)
        is_int = isinstance(index, int)
        if not is_str and not is_int:
            raise TypeError(
                'Index must be either str or int, got: {}'.format(type(index)))
        try:
            return self.indexes[index]
        except KeyError as e:
            if is_int and create_dummy:
                logger.warning(
                    'Index %s not found in space %s/%s. Creating dummy.',
                    index, self.sid, self.name
                )
                idx = SchemaDummyIndex.new()
                idx.iid = index
                idx.sid = self.sid
                idx.name = str(index)
                self.indexes[index] = idx
                return idx
            else:
                raise TarantoolSchemaError(
                    'Index {} not found in space {}/{}'.format(
                        index, self.sid, self.name
                    )
                ) from None

    def __repr__(self):
        return '<{} id={}, name={}, arity={}>'.format(
            self.__class__.__name__,
            self.sid, self.name, self.arity
        )


cdef class SchemaDummySpace(SchemaSpace):
    pass


cdef class Schema:
    @staticmethod
    cdef Schema new(int64_t schema_id):
        cdef Schema s
        s = Schema.__new__(Schema)
        s.id = schema_id
        s.schema = {}
        return s

    cdef SchemaSpace get_space(self, space):
        try:
            return self.schema[space]
        except KeyError:
            return None

    cdef SchemaIndex get_index(self, space, index):
        sp = self.get_space(space)
        if sp is None:
            return None
        try:
            return sp.indexes[index]
        except KeyError:
            return None

    cdef SchemaSpace get_or_create_space(self, space):
        cdef:
            bint is_str, is_int
        is_str = isinstance(space, str)
        is_int = isinstance(space, int)
        if not is_str and not is_int:
            raise TypeError(
                'Space must be either str or int, got: {}'.format(type(space)))

        try:
            return self.schema[space]
        except KeyError:
            if is_str:
                raise TarantoolSchemaError(
                    'Space {} not found'.format(space)
                ) from None
            else:
                return self.create_dummy_space(space)

    cdef SchemaSpace create_dummy_space(self, int space_id):
        cdef SchemaSpace s
        logger.warning('Space %s not found. Creating dummy.', space_id)
        s = SchemaDummySpace.new()
        s.sid = space_id
        s.name = str(space_id)
        self.schema[space_id] = s
        return s

    cdef inline clear(self):
        self.schema.clear()

    cdef SchemaSpace parse_space(self, list space_row):
        cdef:
            SchemaSpace sp
            size_t k
            size_t row_len
            list format_list
            TntField f

        sp = SchemaSpace.new()
        row_len = len(space_row)

        k = 0
        sp.sid = space_row[k]
        k += 1
        if k < row_len:
            sp.owner = space_row[k]
        k += 1
        if k < row_len:
            sp.name = space_row[k]
        k += 1
        if k < row_len:
            sp.engine = space_row[k]
        k += 1
        if k < row_len:
            sp.field_count = space_row[k]
        k += 1
        if k < row_len:
            sp.flags = space_row[k]
        k += 1
        if k < row_len:
            # format
            format_list = space_row[k]
            for i in range(len(format_list)):
                field_id = i
                field_name = format_list[i]['name']
                field_type = format_list[i]['type']
                f = TntField.new(field_id, field_name, field_type)

                sp.fields_map[field_name] = f
                sp.fields.append(field_name)

        return sp

    cdef SchemaIndex parse_index(self, list index_row):
        cdef:
            SchemaIndex idx
            SchemaSpace sp

        idx = SchemaIndex.new()
        idx.sid = index_row[0]
        idx.iid = index_row[1]
        idx.name = index_row[2]
        idx.index_type = index_row[3]
        idx.unique = index_row[4]
        idx.parts = []
        idx.fields = []

        sp = self.get_space(idx.sid)
        if sp is None:
            raise TarantoolSchemaError(
                'Space with id {} not found'.format(idx.sid))

        parts = index_row[5]
        if isinstance(parts, (list, tuple)):
            for field_id, field_type in parts:
                idx.parts.append((field_id, field_type))

                if field_id < len(sp.fields):
                    idx.fields.append(
                        <TntField>sp.fields[field_id]
                    )
                else:
                    logger.warning(
                        'Field #%d of space %s is not '
                        'in space format definition', field_id, sp.name)
        else:
            for i in range(index_row[5]):
                field_id = index_row[5 + 1 + i * 2]
                field_type = index_row[5 + 2 + i * 2]

                idx.parts.append((field_id, field_type))

                if field_id < len(sp.fields):
                    idx.fields.append(
                        <TntField>sp.fields[field_id]
                    )
                else:
                    logger.warning(
                        'Field #%d of space %s is not '
                        'in space format definition', field_id, sp.name)

        return idx

    @staticmethod
    cdef Schema parse(int64_t schema_id, spaces, indexes):
        cdef:
            Schema s
            SchemaSpace sp
            SchemaIndex idx

        s = Schema.new(schema_id)
        for space_row in spaces:
            sp = s.parse_space(space_row)
            s.schema[sp.sid] = sp
            if sp.name:
                s.schema[sp.name] = sp

        for index_row in indexes:
            idx = s.parse_index(index_row)
            sp = s.schema[idx.sid]
            sp.add_index(idx)
        return s

    def __repr__(self):
        return '<Schema>'


cdef list dict_to_list_fields(list fields, dict d, bint default_none):
    cdef:
        list l
        dict used
        object value, field
        int32_t used_count

    l = []
    used_count = len(d)

    for field in fields:
        try:
            value = d[field]
            used_count -= 1
            l.append(value)
        except KeyError as e:
            if default_none:
                l.append(None)

    # Warn user if he used any of unknown fields
    if used_count != 0:
        used = {}
        for field in fields:
            if field in d:
                used[field] = None
        if '' in d:
            used[''] = None

        for field in d:
            if field not in used:
                logger.warning(
                    'Field \'%s\' in supplied dict is unknown as '
                    'a tuple field for selected index. Skipping.',
                    field
                )
    return l

