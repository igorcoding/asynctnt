from libc.stdint cimport uint64_t, uint32_t

cdef class Db:
    cdef:
        BaseProtocol _protocol
        bytes _encoding

    @staticmethod
    cdef inline Db new(BaseProtocol protocol)

    cdef inline uint64_t next_sync(self)

    cdef object _ping(self, float timeout)

    cdef object _call(self, tarantool.iproto_type op, str func_name, args,
                      float timeout, bint push_subscribe)

    cdef object _eval(self, str expression, args,
                      float timeout, bint push_subscribe)

    cdef object _select(self, object space, object index, object key,
                        uint64_t offset, uint64_t limit, uint32_t iterator,
                        float timeout, bint push_subscribe)

    # cdef object _insert(self, SchemaSpace space, t, bint replace,
    #                     float timeout, bint push_subscribe)
    #
    # cdef object _delete(self, SchemaSpace space, SchemaIndex index, key,
    #                     float timeout, bint push_subscribe)
    #
    # cdef object _update(self, SchemaSpace space, SchemaIndex index, key,
    #                     list operations, float timeout, bint push_subscribe)
    #
    # cdef object _upsert(self, SchemaSpace space, t, list operations,
    #                     float timeout, bint push_subscribe)
    #
    # cdef object _sql(self, str query, args, bint parse_metadata,
    #                  float timeout, bint push_subscribe)
    #
    # cdef object _auth(self, bytes salt, str username, str password,
    #                   float timeout, bint push_subscribe)

    @staticmethod
    cdef bytes _sha1(tuple values)

    @staticmethod
    cdef bytes _strxor(bytes hash1, bytes scramble)
