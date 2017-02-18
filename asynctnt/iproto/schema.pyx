from asynctnt.log import logger
from cpython.mem cimport PyMem_Malloc, PyMem_Free

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
        idx.sid = -1
        idx.iid = -1
        idx.name = None
        idx.index_type = None
        idx.unique = None
        idx.parts = []
        idx.fields_names = []
        return idx

    def __repr__(self):
        return \
            '<SchemaIndex sid={}, id={}, name={}, ' \
            'type={}, unique={}>'.format(
                self.sid, self.iid, self.name, self.index_type, self.unique
            )


cdef class SchemaSpace:

    @staticmethod
    cdef SchemaSpace new(list space_row):
        cdef SchemaSpace sp
        sp = SchemaSpace.__new__(SchemaSpace)
        sp.sid = -1
        sp.owner = -1
        sp.name = None
        sp.engine = None
        sp.field_count = 0
        sp.flags = None

        sp.fields_map = {}
        sp.fields_names = []
        sp.indexes = {}
        return sp

    cdef add_index(self, SchemaIndex idx):
        self.indexes[idx.iid] = idx
        if idx.name:
            self.indexes[idx.name] = idx

    def __repr__(self):
        return '<SchemaSpace id={}, name={}, arity={}>'.format(
            self.sid, self.name, self.arity
        )


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

    cdef inline clear(self):
        self.schema.clear()

    cdef SchemaSpace parse_space(self, list space_row):
        cdef:
            SchemaSpace sp
            size_t k
            size_t row_len
            list format_list
            TntField f

        sp = SchemaSpace.new(space_row)
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
                sp.fields_names.append(field_name)

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
        idx.fields_names = []

        sp = self.get_space(idx.sid)
        if sp is None:
            raise KeyError('Space with id {} not found'.format(idx.sid))

        parts = index_row[5]
        if isinstance(parts, (list, tuple)):
            for field_id, field_type in parts:
                idx.parts.append((field_id, field_type))

                if field_id < len(sp.fields_names):
                    idx.fields_names.append(
                        <TntField>sp.fields_names[field_id]
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

                if field_id < len(sp.fields_names):
                    idx.fields_names.append(
                        <TntField>sp.fields_names[field_id]
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
