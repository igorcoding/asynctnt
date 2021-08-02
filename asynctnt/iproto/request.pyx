cimport cython
from libc.stdint cimport uint64_t, int64_t


@cython.final
@cython.freelist(REQUEST_FREELIST)
cdef class Request:
    @staticmethod
    cdef inline Request new(tarantool.iproto_type op,
                            uint64_t sync, int64_t schema_id,
                            SchemaSpace space, bint push_subscribe,
                            bint check_schema_change):
        cdef Request req
        req = Request.__new__(Request)
        req.op = op
        req.sync = sync
        req.schema_id = schema_id
        req.space = space
        req.waiter = None
        req.timeout_handle = None
        req.parse_as_tuples = space is not None
        req.parse_metadata = True
        req.push_subscribe = push_subscribe
        req.response = None
        req.check_schema_change = check_schema_change
        return req

    def __repr__(self):  # pragma: nocover
        return \
            '<Request op={} sync={} schema_id={} push_subscribe={}>'.format(
                self.op,
                self.sync,
                self.schema_id,
                self.push_subscribe
            )
