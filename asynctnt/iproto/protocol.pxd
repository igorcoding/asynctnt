cimport asynctnt.iproto.tupleobj as tupleobj
cimport asynctnt.iproto.tarantool as tarantool

include "const.pxi"

include "cmsgpuck.pxd"
include "xd.pxd"
include "python.pxd"

include "unicodeutil.pxd"
include "schema.pxd"
include "ext.pxd"
include "buffer.pxd"
include "rbuffer.pxd"

include "requests/base.pxd"
include "requests/ping.pxd"
include "requests/call.pxd"
include "requests/eval.pxd"
include "requests/select.pxd"
include "requests/insert.pxd"
include "requests/delete.pxd"
include "requests/update.pxd"
include "requests/upsert.pxd"
include "requests/prepare.pxd"
include "requests/execute.pxd"
include "requests/auth.pxd"

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

    cdef object _new_waiter_for_request(self, Response response, BaseRequest req, float timeout)
    cdef Db _create_db(self)
    cdef object execute(self, BaseRequest req, WriteBuffer buf, float timeout)
