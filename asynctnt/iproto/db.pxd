from libc.stdint cimport uint64_t, uint32_t, int64_t


cdef class Db:
    cdef:
        BaseProtocol _protocol
        bytes _encoding

    @staticmethod
    cdef inline Db new(BaseProtocol protocol)

    cdef inline uint64_t next_sync(self)
    # cdef execute(self, Request req, float timeout)

    cdef Request _ping(self)
    cdef Request _call16(self, str func_name, args)
    cdef Request _call(self, str func_name, args)
    cdef Request _eval(self, str expression, args)
    cdef Request _select(self, SchemaSpace space, SchemaIndex index, key,
                         uint64_t offset, uint64_t limit, uint32_t iterator)
    cdef Request _insert(self, SchemaSpace space, t, bint replace)
    cdef Request _delete(self, SchemaSpace space, SchemaIndex index, key)
    cdef Request _update(self, SchemaSpace space, SchemaIndex index, key,
                         list operations)
    cdef Request _upsert(self, SchemaSpace space, t, list operations)
    cdef Request _auth(self, bytes salt, str username, str password)

    @staticmethod
    cdef bytes _sha1(tuple values)

    @staticmethod
    cdef bytes _strxor(bytes hash1, bytes scramble)
