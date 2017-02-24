cimport cpython
cimport tnt

import hashlib
from asynctnt.exceptions import TarantoolDatabaseError, ErrorCode


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

    async def execute(self, Request req, float timeout, bint tuple_as_dict):
        cdef object fut

        if tuple_as_dict is None:
            tuple_as_dict = self._protocol.tuple_as_dict
        req.tuple_as_dict = tuple_as_dict
        try:
            return await self._protocol.execute(req, timeout)
        except TarantoolDatabaseError as e:
            if e.code == ErrorCode.ER_WRONG_SCHEMA_VERSION:
                await self._protocol.refetch_schema()

                # Retry request with updated schema_id
                req.schema_id = self._protocol._schema_id
                req.buf.change_schema_id(req.schema_id)
                return await self._protocol.execute(req, timeout)
            raise

    cdef Request _ping(self):
        cdef:
            tnt.tp_request_type op
            uint64_t sync
            int64_t schema_id
            WriteBuffer buf

        op = tnt.TP_PING
        sync = self.next_sync()
        schema_id = self._protocol._schema_id
        buf = WriteBuffer.new(self._encoding)
        buf.write_header(sync, op, schema_id)
        buf.write_length()
        return Request.new(op, sync, schema_id, buf, None)

    cdef Request _call16(self, str func_name, args):
        cdef:
            tnt.tp_request_type op
            uint64_t sync
            int64_t schema_id
            WriteBuffer buf

        op = tnt.TP_CALL_16
        sync = self.next_sync()
        schema_id = self._protocol._schema_id
        buf = WriteBuffer.new(self._encoding)
        buf.write_header(sync, op, schema_id)
        buf.encode_request_call(func_name, args)
        buf.write_length()
        return Request.new(op, sync, schema_id, buf, None)

    cdef Request _call(self, str func_name, args):
        cdef:
            tnt.tp_request_type op
            uint64_t sync
            int64_t schema_id
            WriteBuffer buf

        op = tnt.TP_CALL
        sync = self.next_sync()
        schema_id = self._protocol._schema_id
        buf = WriteBuffer.new(self._encoding)
        buf.write_header(sync, op, schema_id)
        buf.encode_request_call(func_name, args)
        buf.write_length()
        return Request.new(op, sync, schema_id, buf, None)

    cdef Request _eval(self, str expression, args):
        cdef:
            tnt.tp_request_type op
            uint64_t sync
            int64_t schema_id
            WriteBuffer buf

        op = tnt.TP_EVAL
        sync = self.next_sync()
        schema_id = self._protocol._schema_id
        buf = WriteBuffer.new(self._encoding)
        buf.write_header(sync, op, schema_id)
        buf.encode_request_eval(expression, args)
        buf.write_length()
        return Request.new(op, sync, schema_id, buf, None)

    cdef Request _select(self, SchemaSpace space, SchemaIndex index, key,
                         uint64_t offset, uint64_t limit, uint32_t iterator):
        cdef:
            tnt.tp_request_type op
            uint64_t sync
            int64_t schema_id
            WriteBuffer buf

        op = tnt.TP_SELECT
        sync = self.next_sync()
        schema_id = self._protocol._schema_id
        buf = WriteBuffer.new(self._encoding)
        buf.write_header(sync, op, schema_id)
        buf.encode_request_select(space, index, key,
                                  offset, limit, iterator)
        buf.write_length()
        return Request.new(op, sync, schema_id, buf, space)

    cdef Request _insert(self, SchemaSpace space, t, bint replace):
        cdef:
            tnt.tp_request_type op
            uint64_t sync
            int64_t schema_id
            WriteBuffer buf

        op = tnt.TP_INSERT if not replace else tnt.TP_REPLACE
        sync = self.next_sync()
        schema_id = self._protocol._schema_id
        buf = WriteBuffer.new(self._encoding)
        buf.write_header(sync, op, schema_id)
        buf.encode_request_insert(space, t)
        buf.write_length()
        return Request.new(op, sync, schema_id, buf, space)

    cdef Request _delete(self, SchemaSpace space, SchemaIndex index, key):
        cdef:
            tnt.tp_request_type op
            uint64_t sync
            int64_t schema_id
            WriteBuffer buf

        op = tnt.TP_DELETE
        sync = self.next_sync()
        schema_id = self._protocol._schema_id
        buf = WriteBuffer.new(self._encoding)
        buf.write_header(sync, op, schema_id)
        buf.encode_request_delete(space, index, key)
        buf.write_length()
        return Request.new(op, sync, schema_id, buf, space)

    cdef Request _update(self, SchemaSpace space, SchemaIndex index,
                         key, list operations):
        cdef:
            tnt.tp_request_type op
            uint64_t sync
            int64_t schema_id
            WriteBuffer buf

        op = tnt.TP_UPDATE
        sync = self.next_sync()
        schema_id = self._protocol._schema_id
        buf = WriteBuffer.new(self._encoding)
        buf.write_header(sync, op, schema_id)
        buf.encode_request_update(space, index, key, operations)
        buf.write_length()
        return Request.new(op, sync, schema_id, buf, space)

    cdef Request _upsert(self, SchemaSpace space, t, list operations):
        cdef:
            tnt.tp_request_type op
            uint64_t sync
            int64_t schema_id
            WriteBuffer buf

        op = tnt.TP_UPSERT
        sync = self.next_sync()
        schema_id = self._protocol._schema_id
        buf = WriteBuffer.new(self._encoding)
        buf.write_header(sync, op, schema_id)
        buf.encode_request_upsert(space, t, operations)
        buf.write_length()
        return Request.new(op, sync, schema_id, buf, space)

    cdef Request _auth(self, bytes salt, str username, str password):
        cdef:
            tnt.tp_request_type op
            uint64_t sync
            int64_t schema_id
            WriteBuffer buf

            bytes username_bytes, password_bytes
            bytes hash1, hash2, scramble

        op = tnt.TP_AUTH
        sync = self.next_sync()
        schema_id = self._protocol._schema_id
        buf = WriteBuffer.new(self._encoding)
        buf.write_header(sync, op, schema_id)

        username_bytes = encode_unicode_string(username, self._encoding)
        password_bytes = encode_unicode_string(password, self._encoding)

        hash1 = Db._sha1((password_bytes,))
        hash2 = Db._sha1((hash1,))
        scramble = Db._sha1((salt, hash2))
        scramble = Db._strxor(hash1, scramble)

        buf.encode_request_auth(username_bytes, scramble)

        buf.write_length()
        return Request.new(op, sync, schema_id, buf, None)

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

    def ping(self, timeout=-1):
        return self.execute(
            self._ping(),
            timeout, False
        )

    def call16(self, func_name, args=None, timeout=-1):
        return self.execute(
            self._call16(func_name, args),
            timeout, False
        )

    def call(self, func_name, args=None, timeout=-1):
        return self.execute(
            self._call(func_name, args),
            timeout, False
        )

    def eval(self, expression, args=None, timeout=-1):
        return self.execute(
            self._eval(expression, args),
            timeout, False
        )

    def select(self, space, key=None,
               offset=0, limit=0xffffffff, index=0, iterator=0,
               timeout=-1, tuple_as_dict=None):
        cdef:
            SchemaSpace sp
            SchemaIndex idx
        sp = self._protocol._schema.get_or_create_space(space)
        idx = sp.get_index(index)
        iterator = self._protocol.transform_iterator(iterator)

        return self.execute(
            self._select(sp, idx, key, offset, limit, iterator),
            timeout, tuple_as_dict
        )

    def insert(self, space, t, replace=False,
               timeout=-1, tuple_as_dict=None):
        cdef:
            SchemaSpace sp
        sp = self._protocol._schema.get_or_create_space(space)

        return self.execute(
            self._insert(sp, t, replace),
            timeout, tuple_as_dict
        )

    def replace(self, space, t, timeout=-1, tuple_as_dict=None):
        cdef:
            SchemaSpace sp
        sp = self._protocol._schema.get_or_create_space(space)

        return self.execute(
            self._insert(sp, t, True),
            timeout, tuple_as_dict
        )

    def delete(self, space, key, index=0, timeout=-1, tuple_as_dict=None):
        cdef:
            SchemaSpace sp
            SchemaIndex idx
        sp = self._protocol._schema.get_or_create_space(space)
        idx = sp.get_index(index)

        return self.execute(
            self._delete(sp, idx, key),
            timeout, tuple_as_dict
        )

    def update(self, space, key, operations, index=0,
               timeout=-1, tuple_as_dict=None):
        cdef:
            SchemaSpace sp
            SchemaIndex idx
        sp = self._protocol._schema.get_or_create_space(space)
        idx = sp.get_index(index)

        return self.execute(
            self._update(sp, idx, key, operations),
            timeout, tuple_as_dict
        )

    def upsert(self, space, t, operations, timeout=-1, tuple_as_dict=None):
        cdef:
            SchemaSpace sp
        sp = self._protocol._schema.get_or_create_space(space)

        return self.execute(
            self._upsert(sp, t, operations),
            timeout, tuple_as_dict
        )
