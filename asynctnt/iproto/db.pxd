cimport cython
from libc.stdint cimport uint32_t, uint64_t


@cython.final
cdef class Db:
    cdef:
        uint64_t _stream_id
        BaseProtocol _protocol
        bytes _encoding

    @staticmethod
    cdef inline Db create(BaseProtocol protocol, uint64_t stream_id)

    cdef inline uint64_t next_sync(self)

    cdef object _ping(self, float timeout)

    cdef object _id(self, float timeout)

    cdef object _auth(self,
                      bytes salt,
                      str username,
                      str password,
                      float timeout)

    cdef object _call(self,
                      tarantool.iproto_type op,
                      str func_name,
                      object args,
                      float timeout,
                      bint push_subscribe)

    cdef object _eval(self,
                      str expression,
                      object args,
                      float timeout,
                      bint push_subscribe)

    cdef object _select(self,
                        object space,
                        object index,
                        object key,
                        uint64_t offset,
                        uint64_t limit,
                        object iterator,
                        float timeout,
                        bint check_schema_change)

    cdef object _insert(self,
                        object space,
                        object t,
                        bint replace,
                        float timeout)

    cdef object _delete(self,
                        object space,
                        object index,
                        object key,
                        float timeout)

    cdef object _update(self,
                        object space,
                        object index,
                        object key,
                        list operations,
                        float timeout)

    cdef object _upsert(self,
                        object space,
                        object t,
                        list operations,
                        float timeout)

    cdef object _execute(self,
                         query,
                         object args,
                         bint parse_metadata,
                         float timeout)

    cdef object _prepare(self,
                         query,
                         bint parse_metadata,
                         float timeout)

    cdef object _begin(self,
                       uint32_t isolation,
                       double tx_timeout,
                       float timeout)

    cdef object _commit(self, float timeout)

    cdef object _rollback(self, float timeout)
