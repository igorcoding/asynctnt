cimport cpython

import hashlib

cdef class Db:
    def __cinit__(self):
        self._protocol = None
        self._encoding = None

    @staticmethod
    cdef inline Db new(BaseProtocol protocol):
        cdef Db db = Db.__new__(Db)
        db._protocol = protocol
        db._encoding = protocol.encoding
        return db

    cdef inline uint64_t next_sync(self):
        return self._protocol.next_sync()

    #
    # cdef object _insert(self, SchemaSpace space, t, bint replace,
    #                     float timeout, bint push_subscribe):
    #     cdef:
    #         tarantool.iproto_type op
    #         uint64_t sync
    #         int64_t schema_id
    #         WriteBuffer buf
    #
    #     op = tarantool.IPROTO_REPLACE if replace else tarantool.IPROTO_INSERT
    #     sync = self.next_sync()
    #     schema_id = -1  # not sending schema_id with the request
    #     buf = WriteBuffer.new(self._encoding)
    #     buf.write_header(sync, op, schema_id)
    #     buf.encode_request_insert(space, t)
    #     buf.write_length()
    #     return self._protocol.execute(
    #         Request.new(op, sync, schema_id, space, push_subscribe),
    #         buf, timeout)
    #
    # cdef object _delete(self, SchemaSpace space, SchemaIndex index, key,
    #                     float timeout, bint push_subscribe):
    #     cdef:
    #         tarantool.iproto_type op
    #         uint64_t sync
    #         int64_t schema_id
    #         WriteBuffer buf
    #
    #     op = tarantool.IPROTO_DELETE
    #     sync = self.next_sync()
    #     schema_id = -1  # not sending schema_id with the request
    #     buf = WriteBuffer.new(self._encoding)
    #     buf.write_header(sync, op, schema_id)
    #     buf.encode_request_delete(space, index, key)
    #     buf.write_length()
    #     return self._protocol.execute(
    #         Request.new(op, sync, schema_id, space, push_subscribe),
    #         buf, timeout)
    #
    # cdef object _update(self, SchemaSpace space, SchemaIndex index,
    #                     key, list operations,
    #                     float timeout, bint push_subscribe):
    #     cdef:
    #         tarantool.iproto_type op
    #         uint64_t sync
    #         int64_t schema_id
    #         WriteBuffer buf
    #
    #     op = tarantool.IPROTO_UPDATE
    #     sync = self.next_sync()
    #     schema_id = -1  # not sending schema_id with the request
    #     buf = WriteBuffer.new(self._encoding)
    #     buf.write_header(sync, op, schema_id)
    #     buf.encode_request_update(space, index, key, operations)
    #     buf.write_length()
    #     return self._protocol.execute(
    #         Request.new(op, sync, schema_id, space, push_subscribe),
    #         buf, timeout)
    #
    # cdef object _upsert(self, SchemaSpace space, t, list operations,
    #                     float timeout, bint push_subscribe):
    #     cdef:
    #         tarantool.iproto_type op
    #         uint64_t sync
    #         int64_t schema_id
    #         WriteBuffer buf
    #
    #     op = tarantool.IPROTO_UPSERT
    #     sync = self.next_sync()
    #     schema_id = -1  # not sending schema_id with the request
    #     buf = WriteBuffer.new(self._encoding)
    #     buf.write_header(sync, op, schema_id)
    #     buf.encode_request_upsert(space, t, operations)
    #     buf.write_length()
    #     return self._protocol.execute(
    #         Request.new(op, sync, schema_id, space, push_subscribe),
    #         buf, timeout)
    #
    # cdef object _sql(self, str query, args, bint parse_metadata,
    #                  float timeout, bint push_subscribe):
    #     cdef:
    #         tarantool.iproto_type op
    #         uint64_t sync
    #         int64_t schema_id
    #         WriteBuffer buf
    #         Request req
    #
    #     op = tarantool.IPROTO_EXECUTE
    #     sync = self.next_sync()
    #     schema_id = -1  # not sending schema_id with the request
    #     buf = WriteBuffer.new(self._encoding)
    #     buf.write_header(sync, op, schema_id)
    #     buf.encode_request_sql(query, args)
    #     buf.write_length()
    #     req = Request.new(op, sync, schema_id, None, push_subscribe)
    #     req.parse_metadata = parse_metadata
    #     req.parse_as_tuples = True
    #     return self._protocol.execute(req, buf, timeout)
    #
    # cdef object _auth(self, bytes salt, str username, str password,
    #                   float timeout, bint push_subscribe):
    #     cdef:
    #         tarantool.iproto_type op
    #         uint64_t sync
    #         int64_t schema_id
    #         WriteBuffer buf
    #
    #         bytes username_bytes, password_bytes
    #         bytes hash1, hash2, scramble
    #
    #     op = tarantool.IPROTO_AUTH
    #     sync = self.next_sync()
    #     schema_id = -1  # not sending schema_id with the request
    #     buf = WriteBuffer.new(self._encoding)
    #     buf.write_header(sync, op, schema_id)
    #
    #     username_bytes = encode_unicode_string(username, self._encoding)
    #     password_bytes = encode_unicode_string(password, self._encoding)
    #
    #     hash1 = Db._sha1((password_bytes,))
    #     hash2 = Db._sha1((hash1,))
    #     scramble = Db._sha1((salt, hash2))
    #     scramble = Db._strxor(hash1, scramble)
    #
    #     buf.encode_request_auth(username_bytes, scramble)
    #
    #     buf.write_length()
    #     return self._protocol.execute(
    #         Request.new(op, sync, schema_id, None, push_subscribe),
    #         buf, timeout)

    @staticmethod
    cdef bytes _sha1(tuple values):
        cdef object sha = hashlib.sha1()
        for i in values:
            if i is not None:
                sha.update(i)
        return sha.digest()

    @staticmethod
    cdef bytes _strxor(bytes hash1, bytes scramble):
        cdef:
            char *hash1_str
            ssize_t hash1_len

            char *scramble_str
            ssize_t scramble_len
        cpython.bytes.PyBytes_AsStringAndSize(hash1,
                                              &hash1_str, &hash1_len)
        cpython.bytes.PyBytes_AsStringAndSize(scramble,
                                              &scramble_str, &scramble_len)
        for i in range(scramble_len):
            scramble_str[i] = hash1_str[i] ^ scramble_str[i]
        return scramble

    # public methods

    cdef object _ping(self, float timeout):
        cdef PingRequest req = PingRequest.__new__(PingRequest)
        req.op = tarantool.IPROTO_PING
        req.sync = self.next_sync()
        return self._protocol.execute(
            req,
            req.encode(self._encoding),
            <float> timeout
        )

    cdef object _call(self, tarantool.iproto_type op, str func_name, args,
                      float timeout, bint push_subscribe):
        cdef CallRequest req = CallRequest.__new__(CallRequest)
        req.op = op
        req.sync = self.next_sync()
        req.func_name = func_name
        req.args = args
        req.push_subscribe = push_subscribe
        return self._protocol.execute(
            req,
            req.encode(self._encoding),
            <float> timeout
        )

    cdef object _eval(self, str expression, args,
                      float timeout, bint push_subscribe):
        cdef EvalRequest req = EvalRequest.__new__(EvalRequest)
        req.op = tarantool.IPROTO_EVAL
        req.sync = self.next_sync()
        req.expression = expression
        req.args = args
        req.push_subscribe = push_subscribe
        return self._protocol.execute(
            req,
            req.encode(self._encoding),
            <float> timeout
        )

    cdef object _select(self, object space, object index, key,
                        uint64_t offset, uint64_t limit, uint32_t iterator,
                        float timeout, bint push_subscribe):
        cdef:
            SchemaSpace sp
            SchemaIndex idx
            SelectRequest req

        sp = self._protocol._schema.get_or_create_space(space)
        idx = sp.get_index(index)

        iterator = self._protocol.transform_iterator(iterator)
        if key is None and iterator == 0:
            iterator = 2  # ALL

        req = SelectRequest.__new__(SelectRequest)
        req.op = tarantool.IPROTO_SELECT
        req.sync = self.next_sync()
        req.space = sp
        req.index = idx
        req.key = key
        req.offset = offset
        req.limit = limit
        req.iterator = iterator
        req.push_subscribe = push_subscribe
        req.parse_as_tuples = True

        return self._protocol.execute(
            req,
            req.encode(self._encoding),
            <float> timeout
        )

    def ping(self, timeout = -1):
        return self._ping(timeout)

    def call16(self, func_name, args=None, timeout=-1, push_subscribe=False):
        return self._call(tarantool.IPROTO_CALL_16, func_name, args, timeout, push_subscribe)

    def call(self, func_name, args=None, timeout=-1, push_subscribe=False):
        return self._call(tarantool.IPROTO_CALL, func_name, args, timeout, push_subscribe)

    def eval(self, expression, args=None, timeout=-1, push_subscribe=False):
        return self._eval(expression, args, timeout, push_subscribe)

    def select(self, space, key=None,
               offset=0, limit=0xffffffff, index=0, iterator=0,
               timeout=-1, push_subscribe=False):
        return self._select(space, index, key, offset, limit, iterator,
                            timeout, push_subscribe)

    # def insert(self, space, t, replace=False,
    #            timeout=-1):
    #     cdef:
    #         SchemaSpace sp
    #     sp = self._protocol._schema.get_or_create_space(space)
    #
    #     return self._insert(sp, t, replace, timeout, False)
    #
    # def replace(self, space, t, timeout=-1):
    #     cdef:
    #         SchemaSpace sp
    #     sp = self._protocol._schema.get_or_create_space(space)
    #
    #     return self._insert(sp, t, True, timeout, False)
    #
    # def delete(self, space, key, index=0, timeout=-1):
    #     cdef:
    #         SchemaSpace sp
    #         SchemaIndex idx
    #     sp = self._protocol._schema.get_or_create_space(space)
    #     idx = sp.get_index(index)
    #
    #     return self._delete(sp, idx, key, timeout, False)
    #
    # def update(self, space, key, operations, index=0, timeout=-1):
    #     cdef:
    #         SchemaSpace sp
    #         SchemaIndex idx
    #     sp = self._protocol._schema.get_or_create_space(space)
    #     idx = sp.get_index(index)
    #
    #     return self._update(sp, idx, key, operations, timeout, False)
    #
    # def upsert(self, space, t, operations, timeout=-1):
    #     cdef:
    #         SchemaSpace sp
    #     sp = self._protocol._schema.get_or_create_space(space)
    #
    #     return self._upsert(sp, t, operations, timeout, False)
    #
    # def sql(self, query, args, parse_metadata=True, timeout=-1):
    #     return self._sql(query, args, parse_metadata, timeout, False)
