cimport asynctnt.iproto.tupleobj as tupleobj
cimport asynctnt.iproto.tarantool as tarantool

include "const.pxi"

include "cmsgpuck.pxd"
include "python.pxd"

include "unicodeutil.pxd"
include "schema.pxd"
include "buffer.pxd"
include "rbuffer.pxd"
include "request.pxd"
include "response.pxd"
include "db.pxd"
include "push.pxd"

include "coreproto.pxd"


cdef class BaseProtocol(CoreProtocol):
    cdef:
        object loop
        str username
        str password
        bint fetch_schema
        bint auto_refetch_schema
        float request_timeout

        object connected_fut
        object on_connection_made_cb
        object on_connection_lost_cb

        object _on_request_completed_cb
        object _on_request_timeout_cb

        dict _reqs
        uint64_t _sync
        Schema _schema
        int64_t _schema_id
        bint _schema_fetch_in_progress
        object _refetch_schema_future
        Db _db

        object create_future

    cdef void _set_connection_ready(self)
    cdef void _set_connection_error(self, e)

    cdef void _do_auth(self, str username, str password)
    cdef void _do_fetch_schema(self, object fut)
    cdef object _refetch_schema(self)

    cdef inline uint64_t next_sync(self)
    cdef uint32_t transform_iterator(self, iterator) except *

    cdef object _new_waiter_for_request(self, Request req, float timeout)
    cdef Db _create_db(self)
    cdef object execute(self, Request req, WriteBuffer buf, float timeout)
