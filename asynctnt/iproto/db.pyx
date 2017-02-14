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

    async def execute(self, Request req, float timeout):
        cdef object fut
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
        return Request.new(op, sync, schema_id, buf)

    cdef Request _call16(self, str func_name, list args):
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
        return Request.new(op, sync, schema_id, buf)

    cdef Request _call(self, str func_name, list args):
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
        return Request.new(op, sync, schema_id, buf)

    cdef Request _eval(self, str expression, list args):
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
        return Request.new(op, sync, schema_id, buf)

    cdef Request _select(self, uint32_t space, uint32_t index, list key,
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
        return Request.new(op, sync, schema_id, buf)

    cdef Request _insert(self, uint32_t space, list t, bint replace):
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
        return Request.new(op, sync, schema_id, buf)

    cdef Request _delete(self, uint32_t space, uint32_t index, list key):
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
        return Request.new(op, sync, schema_id, buf)

    cdef Request _update(self, uint32_t space, uint32_t index,
                         list key, list operations):
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
        return Request.new(op, sync, schema_id, buf)

    cdef Request _upsert(self, uint32_t space, list t, list operations):
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
        return Request.new(op, sync, schema_id, buf)

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
        return Request.new(op, sync, schema_id, buf)

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
            timeout
        )

    def call16(self, func_name, args=None, timeout=-1):
        return self.execute(
            self._call16(func_name, args),
            timeout
        )

    def call(self, func_name, args=None, timeout=-1):
        return self.execute(
            self._call(func_name, args),
            timeout
        )

    def eval(self, expression, args=None, timeout=-1):
        return self.execute(
            self._eval(expression, args),
            timeout
        )

    def select(self, space, key=None,
                 offset=0, limit=0xffffffff, index=0, iterator=0, timeout=-1):
        space = self._protocol.transform_space(space)
        index = self._protocol.transform_index(space, index)
        iterator = self._protocol.transform_iterator(iterator)

        return self.execute(
            self._select(space, index, key, offset, limit, iterator),
            timeout
        )

    def insert(self, space, t, replace=False, timeout=-1):
        space = self._protocol.transform_space(space)

        return self.execute(
            self._insert(space, t, replace),
            timeout
        )

    def replace(self, space, t, timeout=-1):
        return self.insert(space, t, replace=True, timeout=timeout)

    def delete(self, space, key, index=0, timeout=-1):
        space = self._protocol.transform_space(space)
        index = self._protocol.transform_index(space, index)

        return self.execute(
            self._delete(space, index, key),
            timeout
        )

    def update(self, space, key, operations, index=0, timeout=-1):
        space = self._protocol.transform_space(space)
        index = self._protocol.transform_index(space, index)

        return self.execute(
            self._update(space, index, key, operations),
            timeout
        )

    def upsert(self, space, t, operations, timeout=-1):
        space = self._protocol.transform_space(space)

        return self.execute(
            self._upsert(space, t, operations),
            timeout
        )
