cdef class SchemaIndex:

    def __init__(self, list index_row):
        self.sid = index_row[0]
        self.iid = index_row[1]
        self.name = index_row[2]
        self.index_type = index_row[3]
        self.unique = index_row[4]
        self.parts = []
        if isinstance(index_row[5], (list, tuple)):
            for k, v in index_row[5]:
                self.parts.append((k, v))
        else:
            for i in range(index_row[5]):
                self.parts.append(
                    (index_row[5 + 1 + i * 2], index_row[5 + 2 + i * 2]))

        def __repr__(self):
            return \
                '<SchemaIndex sid={}, id={}, name={}, ' \
                'type={}, unique={}>'.format(
                    self.sid, self.iid, self.name, self.index_type, self.unique
                )


cdef class SchemaSpace:
    def __init__(self, list space_row):
        self.sid = space_row[0]
        self.arity = space_row[1]
        self.name = space_row[2]
        self.indexes = {}

    cdef add_index(self, SchemaIndex idx):
        self.indexes[idx.iid] = idx
        if idx.name:
            self.indexes[idx.name] = idx

    def __repr__(self):
        return '<SchemaSpace id={}, name={}, arity={}>'.format(
            self.sid, self.name, self.arity
        )


cdef class Schema:
    def __init__(self, int64_t schema_id):
        self.id = schema_id
        self.schema = {}

    cpdef SchemaSpace get_space(self, space):
        try:
            return self.schema[space]
        except KeyError:
            return None

    cpdef SchemaIndex get_index(self, space, index):
        sp = self.get_space(space)
        if sp is None:
            return None
        try:
            return sp.indexes[index]
        except KeyError:
            return None

    cpdef get_id(self):
        return self.id

    cdef inline clear(self):
        self.schema.clear()

    def __repr__(self):
        return '<Schema>'


cdef Schema parse_schema(int64_t schema_id, spaces, indexes):
    cdef:
        Schema s
        SchemaSpace sp
        SchemaIndex idx
    s = Schema(schema_id)
    for space_row in spaces:
        sp = SchemaSpace(space_row)
        s.schema[sp.sid] = sp
        if sp.name:
            s.schema[sp.name] = sp

    for index_row in indexes:
        idx = SchemaIndex(index_row)
        sp = s.schema[idx.sid]
        sp.add_index(idx)
    return s
