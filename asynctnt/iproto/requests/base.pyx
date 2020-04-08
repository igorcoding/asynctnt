cimport cython
from libc.stdint cimport uint64_t, int64_t


@cython.freelist(REQUEST_FREELIST)
cdef class BaseRequest:

    # def __cinit__(self):
    #     self.sync = 0
    #     self.schema_id = -1
    #     self.parse_as_tuples = False
    #     self.parse_metadata = True
    #     self.push_subscribe = False

    def __repr__(self):  # pragma: nocover
        return \
            '<Request op={} sync={} schema_id={} push_subscribe={}>'.format(
                self.op,
                self.sync,
                self.schema_id,
                self.push_subscribe
            )
