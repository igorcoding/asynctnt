cimport cython

@cython.final
cdef class Db:
    def __cinit__(self):
        self._protocol = None
        self._encoding = None

    @staticmethod
    cdef inline Db create(BaseProtocol protocol):
        cdef Db db = Db.__new__(Db)
        db._protocol = protocol
        db._encoding = protocol.encoding
        return db

    cdef inline uint64_t next_sync(self):
        return self._protocol.next_sync()

    cdef object _ping(self, float timeout, bint check_schema_change):
        cdef PingRequest req = PingRequest.__new__(PingRequest)
        req.op = tarantool.IPROTO_PING
        req.sync = self.next_sync()
        req.check_schema_change = check_schema_change
        return self._protocol.execute(
            req,
            req.encode(self._encoding),
            <float> timeout
        )

    cdef object _call(self, tarantool.iproto_type op, str func_name, object args,
                      float timeout, bint push_subscribe, bint check_schema_change):
        cdef CallRequest req = CallRequest.__new__(CallRequest)
        req.op = op
        req.sync = self.next_sync()
        req.func_name = func_name
        req.args = args
        req.push_subscribe = push_subscribe
        req.check_schema_change = check_schema_change
        return self._protocol.execute(
            req,
            req.encode(self._encoding),
            <float> timeout
        )

    cdef object _eval(self, str expression, object args,
                      float timeout, bint push_subscribe, bint check_schema_change):
        cdef EvalRequest req = EvalRequest.__new__(EvalRequest)
        req.op = tarantool.IPROTO_EVAL
        req.sync = self.next_sync()
        req.expression = expression
        req.args = args
        req.push_subscribe = push_subscribe
        req.check_schema_change = check_schema_change
        return self._protocol.execute(
            req,
            req.encode(self._encoding),
            <float> timeout
        )

    cdef object _select(self, object space, object index, object key,
                        uint64_t offset, uint64_t limit, object iterator,
                        float timeout, bint push_subscribe, bint check_schema_change):
        cdef:
            SchemaSpace sp
            SchemaIndex idx
            SelectRequest req
            uint32_t iterator_value

        sp = self._protocol._schema.get_or_create_space(space)
        idx = sp.get_index(index)

        iterator_value = self._protocol.transform_iterator(iterator)
        if key is None and isinstance(iterator, int):
            iterator_value = <uint32_t> iterator
            if iterator_value == 0:
                iterator_value = 2  # ALL

        req = SelectRequest.__new__(SelectRequest)
        req.op = tarantool.IPROTO_SELECT
        req.sync = self.next_sync()
        req.space = sp
        req.index = idx
        req.key = key
        req.offset = offset
        req.limit = limit
        req.iterator = iterator_value
        req.push_subscribe = push_subscribe
        req.check_schema_change = check_schema_change
        req.parse_as_tuples = True

        return self._protocol.execute(
            req,
            req.encode(self._encoding),
            <float> timeout
        )

    cdef object _insert(self, object space, object t, bint replace,
                        float timeout, bint push_subscribe, bint check_schema_change):
        cdef:
            SchemaSpace sp
            InsertRequest req

        sp = self._protocol._schema.get_or_create_space(space)

        req = InsertRequest.__new__(InsertRequest)
        req.op = tarantool.IPROTO_REPLACE if replace else tarantool.IPROTO_INSERT
        req.sync = self.next_sync()
        req.space = sp
        req.t = t
        req.push_subscribe = push_subscribe
        req.check_schema_change = check_schema_change
        req.parse_as_tuples = True

        return self._protocol.execute(
            req,
            req.encode(self._encoding),
            <float> timeout
        )

    cdef object _delete(self, object space, object index, object key,
                        float timeout, bint push_subscribe, bint check_schema_change):
        cdef:
            SchemaSpace sp
            SchemaIndex idx
            DeleteRequest req

        sp = self._protocol._schema.get_or_create_space(space)
        idx = sp.get_index(index)

        req = DeleteRequest.__new__(DeleteRequest)
        req.op = tarantool.IPROTO_DELETE
        req.sync = self.next_sync()
        req.space = sp
        req.index = idx
        req.key = key
        req.push_subscribe = push_subscribe
        req.check_schema_change = check_schema_change
        req.parse_as_tuples = True

        return self._protocol.execute(
            req,
            req.encode(self._encoding),
            <float> timeout
        )

    cdef object _update(self, object space, object index,
                        object key, list operations,
                        float timeout, bint push_subscribe, bint check_schema_change):
        cdef:
            SchemaSpace sp
            SchemaIndex idx
            UpdateRequest req

        sp = self._protocol._schema.get_or_create_space(space)
        idx = sp.get_index(index)

        req = UpdateRequest.__new__(UpdateRequest)
        req.op = tarantool.IPROTO_UPDATE
        req.sync = self.next_sync()
        req.space = sp
        req.index = idx
        req.key = key
        req.operations = operations
        req.push_subscribe = push_subscribe
        req.check_schema_change = check_schema_change
        req.parse_as_tuples = True

        return self._protocol.execute(
            req,
            req.encode(self._encoding),
            <float> timeout
        )

    cdef object _upsert(self, object space, object t, list operations,
                        float timeout, bint push_subscribe, bint check_schema_change):
        cdef:
            SchemaSpace sp
            UpsertRequest req

        sp = self._protocol._schema.get_or_create_space(space)

        req = UpsertRequest.__new__(UpsertRequest)
        req.op = tarantool.IPROTO_UPSERT
        req.sync = self.next_sync()
        req.space = sp
        req.t = t
        req.operations = operations
        req.push_subscribe = push_subscribe
        req.check_schema_change = check_schema_change
        req.parse_as_tuples = True

        return self._protocol.execute(
            req,
            req.encode(self._encoding),
            <float> timeout
        )

    cdef object _execute(self, str query, object args, bint parse_metadata,
                         float timeout, bint push_subscribe, bint check_schema_change):
        cdef:
            ExecuteRequest req

        req = ExecuteRequest.__new__(ExecuteRequest)
        req.op = tarantool.IPROTO_EXECUTE
        req.sync = self.next_sync()
        req.query = query
        req.args = args
        req.push_subscribe = push_subscribe
        req.check_schema_change = check_schema_change
        req.parse_as_tuples = True
        req.parse_metadata = parse_metadata

        return self._protocol.execute(
            req,
            req.encode(self._encoding),
            <float> timeout
        )

    cdef object _auth(self, bytes salt, str username, str password,
                      float timeout, bint push_subscribe, bint check_schema_change):
        cdef:
            AuthRequest req

        req = AuthRequest.__new__(AuthRequest)
        req.op = tarantool.IPROTO_AUTH
        req.sync = self.next_sync()
        req.salt = salt
        req.username = username
        req.password = password
        req.push_subscribe = False
        req.parse_as_tuples = False
        req.parse_metadata = False

        return self._protocol.execute(
            req,
            req.encode(self._encoding),
            <float> timeout
        )

    # public methods

    def ping(self, timeout=-1):
        return self._ping(timeout, <bint> True)

    def call16(self, func_name, args=None, timeout=-1, push_subscribe=False):
        return self._call(tarantool.IPROTO_CALL_16, func_name, args, timeout,
                          <bint> push_subscribe, <bint> True)

    def call(self, func_name, args=None, timeout=-1, push_subscribe=False):
        return self._call(tarantool.IPROTO_CALL, func_name, args, timeout,
                          <bint> push_subscribe, <bint> True)

    def eval(self, expression, args=None, timeout=-1, push_subscribe=False):
        return self._eval(expression, args, timeout,
                          <bint> push_subscribe, <bint> True)

    def select(self, space, key=None,
               offset=0, limit=0xffffffff, index=0, iterator=0,
               timeout=-1, check_schema_change=True):
        return self._select(space, index, key, offset, limit, iterator,
                            timeout, <bint> False, <bint> check_schema_change)

    def insert(self, space, t, replace=False,
               timeout=-1):
        return self._insert(space, t, <bint> replace, timeout,
                            <bint> False, <bint> True)

    def replace(self, space, t, timeout=-1):
        return self._insert(space, t, <bint> True, timeout,
                            <bint> False, <bint> True)

    def delete(self, space, key, index=0, timeout=-1):
        return self._delete(space, index, key, timeout,
                            <bint> False, <bint> True)

    def update(self, space, key, operations, index=0, timeout=-1):
        return self._update(space, index, key, operations, timeout,
                            <bint> False, <bint> True)

    def upsert(self, space, t, operations, timeout=-1):
        return self._upsert(space, t, operations, timeout,
                            <bint> False, <bint> True)
    def execute(self, query, args, parse_metadata=True, timeout=-1):
        return self._execute(query, args, <bint> parse_metadata, timeout,
                             <bint> False, <bint> True)
