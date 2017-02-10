from libc.stdint cimport uint64_t


cdef class Db:
    cdef:
        BaseProtocol _protocol
        bytes _encoding

    @staticmethod
    cdef inline Db new(BaseProtocol protocol)

    cdef inline uint64_t next_sync(self)
