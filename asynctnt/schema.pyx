cdef class SchemaIndex(object):
    cdef public int sid
    cdef public int iid
    cdef public str name
    cdef public str index_type
    cdef public object unique
    cdef public list parts
    
    def __init__(self, index_row):
        self.sid = index_row[0]
        self.iid = index_row[1]
        self.name = index_row[2].decode()
        self.index_type = index_row[3].decode()
        self.unique = index_row[4]
        self.parts = []
        if isinstance(index_row[5], (list, tuple)):
            for k, v in index_row[5]:
                self.parts.append((k, v))
        else:
            for i in range(index_row[5]):
                self.parts.append(
                    (index_row[5 + 1 + i * 2], index_row[5 + 2 + i * 2]))
            

cdef class SchemaSpace:
    cdef public int sid
    cdef public int arity
    cdef public str name
    cdef public dict indexes
    
    def __init__(self, space_row):
        self.sid = space_row[0]
        self.arity = space_row[1]
        self.name = space_row[2].decode()
        self.indexes = {}
            
    cpdef add_index(self, SchemaIndex idx):
        self.indexes[idx.iid] = idx
        if idx.name:
            self.indexes[idx.name] = idx


cdef class Schema(object):
    cdef dict schema
    
    def __init__(self):
        self.schema = {}

    cpdef get_space(self, space):
        try:
            return self.schema[space]
        except KeyError:
            return None

    cpdef get_index(self, space, index):
        sp = self.get_space(space)
        if sp is None:
            return None
        try:
            return sp.indexes[index]
        except KeyError:
            return None

    cdef clear(self):
        self.schema.clear()


def parse_schema(spaces, indexes):
    s = Schema()
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
