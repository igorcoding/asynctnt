from libc.stdint cimport uint64_t, uint32_t

cdef class Db:
    cdef:
        BaseProtocol _protocol
        bytes _encoding

    @staticmethod
    cdef inline Db create(BaseProtocol protocol)

    cdef inline uint64_t next_sync(self)

    cdef object _ping(self, float timeout, bint check_schema_change)

    cdef object _call(self, tarantool.iproto_type op, str func_name, object args,
                      float timeout, bint push_subscribe, bint check_schema_change)

    cdef object _eval(self, str expression, object args,
                      float timeout, bint push_subscribe, bint check_schema_change)

    cdef object _select(self, object space, object index, object key,
                        uint64_t offset, uint64_t limit, object iterator,
                        float timeout, bint push_subscribe, bint check_schema_change)

    cdef object _insert(self, object space, object t, bint replace,
                        float timeout, bint push_subscribe, bint check_schema_change)

    cdef object _delete(self, object space, object index, object key,
                        float timeout, bint push_subscribe, bint check_schema_change)

    cdef object _update(self, object space, object index,
                        object key, list operations,
                        float timeout, bint push_subscribe, bint check_schema_change)

    cdef object _upsert(self, object space, object t, list operations,
                        float timeout, bint push_subscribe, bint check_schema_change)

    cdef object _execute(self, str query, object args, bint parse_metadata,
                         float timeout, bint push_subscribe, bint check_schema_change)

    cdef object _auth(self, bytes salt, str username, str password,
                      float timeout, bint push_subscribe, bint check_schema_change)
