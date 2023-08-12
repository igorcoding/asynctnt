cimport cython


@cython.final
cdef class Db:
    def __cinit__(self):
        self._stream_id = 0
        self._protocol = None
        self._encoding = None

    @staticmethod
    cdef inline Db create(BaseProtocol protocol, uint64_t stream_id):
        cdef Db db = Db.__new__(Db)
        db._stream_id = stream_id
        db._protocol = protocol
        db._encoding = protocol.encoding
        return db

    cdef inline uint64_t next_sync(self):
        return self._protocol.next_sync()

    cdef object _ping(self, float timeout):
        cdef PingRequest req = PingRequest.__new__(PingRequest)
        req.op = tarantool.IPROTO_PING
        req.sync = self.next_sync()
        req.stream_id = self._stream_id
        req.check_schema_change = True
        return self._protocol.execute(self._protocol, req, timeout)

    cdef object _id(self, float timeout):
        cdef IDRequest req = IDRequest.__new__(IDRequest)
        req.op = tarantool.IPROTO_ID
        req.sync = self.next_sync()
        req.stream_id = self._stream_id
        req.check_schema_change = False
        return self._protocol.execute(self._protocol, req, timeout)

    cdef object _auth(self,
                      bytes salt,
                      str username,
                      str password,
                      float timeout):
        cdef:
            AuthRequest req

        req = AuthRequest.__new__(AuthRequest)
        req.op = tarantool.IPROTO_AUTH
        req.sync = self.next_sync()
        req.stream_id = self._stream_id
        req.salt = salt
        req.username = username
        req.password = password
        req.push_subscribe = False
        req.parse_as_tuples = False
        req.parse_metadata = False
        req.check_schema_change = False

        return self._protocol.execute(self._protocol, req, timeout)

    cdef object _call(self,
                      tarantool.iproto_type op,
                      str func_name,
                      object args,
                      float timeout,
                      bint push_subscribe):
        cdef CallRequest req = CallRequest.__new__(CallRequest)
        req.op = op
        req.sync = self.next_sync()
        req.stream_id = self._stream_id
        req.func_name = func_name
        req.args = args
        req.push_subscribe = push_subscribe
        req.check_schema_change = True
        return self._protocol.execute(self._protocol, req, timeout)

    cdef object _eval(self,
                      str expression,
                      object args,
                      float timeout,
                      bint push_subscribe):
        cdef EvalRequest req = EvalRequest.__new__(EvalRequest)
        req.op = tarantool.IPROTO_EVAL
        req.sync = self.next_sync()
        req.stream_id = self._stream_id
        req.expression = expression
        req.args = args
        req.push_subscribe = push_subscribe
        req.check_schema_change = True
        return self._protocol.execute(self._protocol, req, timeout)

    cdef object _select(self,
                        object space,
                        object index,
                        object key,
                        uint64_t offset,
                        uint64_t limit,
                        object iterator,
                        float timeout,
                        bint check_schema_change):
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
        req.stream_id = self._stream_id
        req.space = sp
        req.index = idx
        req.key = key
        req.offset = offset
        req.limit = limit
        req.iterator = iterator_value
        req.push_subscribe = False
        req.check_schema_change = check_schema_change
        req.parse_as_tuples = True

        return self._protocol.execute(self._protocol, req, timeout)

    cdef object _insert(self,
                        object space,
                        object t,
                        bint replace,
                        float timeout):
        cdef:
            SchemaSpace sp
            InsertRequest req

        sp = self._protocol._schema.get_or_create_space(space)

        req = InsertRequest.__new__(InsertRequest)
        req.op = tarantool.IPROTO_REPLACE if replace else tarantool.IPROTO_INSERT
        req.sync = self.next_sync()
        req.stream_id = self._stream_id
        req.space = sp
        req.t = t
        req.push_subscribe = False
        req.check_schema_change = True
        req.parse_as_tuples = True

        return self._protocol.execute(self._protocol, req, timeout)

    cdef object _delete(self,
                        object space,
                        object index,
                        object key,
                        float timeout):
        cdef:
            SchemaSpace sp
            SchemaIndex idx
            DeleteRequest req

        sp = self._protocol._schema.get_or_create_space(space)
        idx = sp.get_index(index)

        req = DeleteRequest.__new__(DeleteRequest)
        req.op = tarantool.IPROTO_DELETE
        req.sync = self.next_sync()
        req.stream_id = self._stream_id
        req.space = sp
        req.index = idx
        req.key = key
        req.push_subscribe = False
        req.check_schema_change = True
        req.parse_as_tuples = True

        return self._protocol.execute(self._protocol, req, timeout)

    cdef object _update(self,
                        object space,
                        object index,
                        object key,
                        list operations,
                        float timeout):
        cdef:
            SchemaSpace sp
            SchemaIndex idx
            UpdateRequest req

        sp = self._protocol._schema.get_or_create_space(space)
        idx = sp.get_index(index)

        req = UpdateRequest.__new__(UpdateRequest)
        req.op = tarantool.IPROTO_UPDATE
        req.sync = self.next_sync()
        req.stream_id = self._stream_id
        req.space = sp
        req.index = idx
        req.key = key
        req.operations = operations
        req.push_subscribe = False
        req.check_schema_change = True
        req.parse_as_tuples = True

        return self._protocol.execute(self._protocol, req, timeout)

    cdef object _upsert(self,
                        object space,
                        object t,
                        list operations,
                        float timeout):
        cdef:
            SchemaSpace sp
            UpsertRequest req

        sp = self._protocol._schema.get_or_create_space(space)

        req = UpsertRequest.__new__(UpsertRequest)
        req.op = tarantool.IPROTO_UPSERT
        req.sync = self.next_sync()
        req.stream_id = self._stream_id
        req.space = sp
        req.t = t
        req.operations = operations
        req.push_subscribe = False
        req.check_schema_change = True
        req.parse_as_tuples = True

        return self._protocol.execute(self._protocol, req, timeout)

    cdef object _execute(self,
                         object query,
                         object args,
                         bint parse_metadata,
                         float timeout):
        cdef:
            ExecuteRequest req

        req = ExecuteRequest.__new__(ExecuteRequest)
        req.op = tarantool.IPROTO_EXECUTE
        req.sync = self.next_sync()
        req.stream_id = self._stream_id

        if isinstance(query, str):
            req.query = query
            req.statement_id = 0
        elif isinstance(query, int):
            req.query = None
            req.statement_id = <uint64_t> query
        else:
            raise TypeError('query must be either str or int')

        req.args = args
        req.parse_metadata = parse_metadata
        req.push_subscribe = False
        req.check_schema_change = True
        req.parse_as_tuples = True

        return self._protocol.execute(self._protocol, req, timeout)

    cdef object _prepare(self,
                         object query,
                         bint parse_metadata,
                         float timeout):
        cdef:
            PrepareRequest req

        req = PrepareRequest.__new__(PrepareRequest)
        req.op = tarantool.IPROTO_PREPARE
        req.sync = self.next_sync()
        req.stream_id = self._stream_id

        if isinstance(query, str):
            req.query = query
            req.statement_id = 0
        elif isinstance(query, int):
            req.query = None
            req.statement_id = <uint64_t> query
        else:
            raise TypeError('query must be either str or int')

        req.push_subscribe = False
        req.check_schema_change = True
        req.parse_as_tuples = True
        req.parse_metadata = parse_metadata

        return self._protocol.execute(self._protocol, req, timeout)

    cdef object _begin(self,
                       uint32_t isolation,
                       double tx_timeout,
                       float timeout):
        cdef BeginRequest req = BeginRequest.__new__(BeginRequest)
        req.op = tarantool.IPROTO_BEGIN
        req.sync = self.next_sync()
        req.stream_id = self._stream_id
        req.check_schema_change = True
        req.push_subscribe = False
        req.isolation = isolation
        req.tx_timeout = tx_timeout
        return self._protocol.execute(self._protocol, req, timeout)

    cdef object _commit(self, float timeout):
        cdef CommitRequest req = CommitRequest.__new__(CommitRequest)
        req.op = tarantool.IPROTO_COMMIT
        req.sync = self.next_sync()
        req.stream_id = self._stream_id
        req.check_schema_change = True
        req.push_subscribe = False
        return self._protocol.execute(self._protocol, req, timeout)

    cdef object _rollback(self, float timeout):
        cdef RollbackRequest req = RollbackRequest.__new__(RollbackRequest)
        req.op = tarantool.IPROTO_ROLLBACK
        req.sync = self.next_sync()
        req.stream_id = self._stream_id
        req.check_schema_change = True
        return self._protocol.execute(self._protocol, req, timeout)

    # public methods

    @property
    def stream_id(self):
        return <int> self._stream_id

    def set_stream_id(self, int stream_id):
        self._stream_id = <uint64_t> stream_id

    def ping(self, float timeout=-1):
        return self._ping(timeout)

    def call16(self,
               str func_name,
               object args=None,
               float timeout=-1,
               bint push_subscribe=False):
        return self._call(tarantool.IPROTO_CALL_16,
                          func_name,
                          args,
                          timeout,
                          <bint> push_subscribe)

    def call(self,
             str func_name,
             object args=None,
             float timeout=-1,
             bint push_subscribe=False):
        return self._call(tarantool.IPROTO_CALL,
                          func_name,
                          args,
                          timeout,
                          <bint> push_subscribe)

    def eval(self,
             str expression,
             object args=None,
             float timeout=-1,
             bint push_subscribe=False):
        return self._eval(expression,
                          args,
                          timeout,
                          <bint> push_subscribe)

    def select(self,
               object space,
               object key=None,
               int offset=0,
               int limit=0xffffffff,
               object index=0,
               object iterator=0,
               float timeout=-1,
               bint check_schema_change=True):
        return self._select(space, index, key, offset, limit, iterator,
                            timeout, check_schema_change)

    def insert(self,
               object space,
               object t,
               bint replace=False,
               float timeout=-1):
        return self._insert(space, t, <bint> replace, timeout)

    def replace(self,
                object space,
                object t,
                float timeout=-1):
        return self._insert(space, t, <bint> True, timeout)

    def delete(self,
               object space,
               object key,
               object index=0,
               float timeout=-1):
        return self._delete(space, index, key, timeout)

    def update(self,
               object space,
               object key,
               list operations,
               object index=0,
               float timeout=-1):
        return self._update(space, index, key, operations, timeout)

    def upsert(self,
               object space,
               object t,
               list operations,
               float timeout=-1):
        return self._upsert(space, t, operations, timeout)

    def execute(self,
                object query,
                object args,
                bint parse_metadata=True,
                float timeout=-1):
        return self._execute(query, args, <bint> parse_metadata, timeout)

    def prepare(self,
                object query,
                bint parse_metadata=True,
                float timeout=-1):
        return self._prepare(query, <bint> parse_metadata, timeout)

    def begin(self,
              uint32_t isolation,
              float tx_timeout=0,
              float timeout=-1):
        return self._begin(isolation, <double> tx_timeout, timeout)

    def commit(self, float timeout=-1):
        return self._commit(timeout)

    def rollback(self, float timeout=-1):
        return self._rollback(timeout)
