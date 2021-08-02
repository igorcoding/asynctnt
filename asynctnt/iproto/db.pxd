from libc.stdint cimport uint64_t, uint32_t


cdef class Db:
    cdef:
        BaseProtocol _protocol
        bytes _encoding

    @staticmethod
    cdef inline Db new(BaseProtocol protocol)

    cdef inline uint64_t next_sync(self)

    cdef object _ping(self, float timeout, bint push_subscribe, bint check_schema_change)

    cdef object _call16(self, str func_name, args,
                        float timeout, bint push_subscribe, bint check_schema_change)

    cdef object _call(self, str func_name, args,
                      float timeout, bint push_subscribe, bint check_schema_change)

    cdef object _eval(self, str expression, args,
                      float timeout, bint push_subscribe, bint check_schema_change)

    cdef object _select(self, SchemaSpace space, SchemaIndex index, key,
                        uint64_t offset, uint64_t limit, uint32_t iterator,
                        float timeout, bint push_subscribe, bint check_schema_change)

    cdef object _insert(self, SchemaSpace space, t, bint replace,
                        float timeout, bint push_subscribe, bint check_schema_change)

    cdef object _delete(self, SchemaSpace space, SchemaIndex index, key,
                        float timeout, bint push_subscribe, bint check_schema_change)

    cdef object _update(self, SchemaSpace space, SchemaIndex index, key,
                        list operations, float timeout, bint push_subscribe, bint check_schema_change)

    cdef object _upsert(self, SchemaSpace space, t, list operations,
                        float timeout, bint push_subscribe, bint check_schema_change)

    cdef object _sql(self, str query, args, bint parse_metadata,
                     float timeout, bint push_subscribe, bint check_schema_change)

    cdef object _auth(self, bytes salt, str username, str password,
                      float timeout, bint push_subscribe, bint check_schema_change)

    @staticmethod
    cdef bytes _sha1(tuple values)

    @staticmethod
    cdef bytes _strxor(bytes hash1, bytes scramble)
