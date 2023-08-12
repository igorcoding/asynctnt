cimport asynctnt.iproto.tarantool as tarantool
cimport asynctnt.iproto.tupleobj as tupleobj

include "const.pxi"

include "cmsgpuck.pxd"
include "xd.pxd"
include "python.pxd"
include "bit.pxd"

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
include "requests/id.pxd"
include "requests/auth.pxd"
include "requests/streams.pxd"

include "response.pxd"
include "db.pxd"
include "push.pxd"

include "coreproto.pxd"

cdef enum PostConnectionState:
    POST_CONNECTION_NONE = 0
    POST_CONNECTION_ID = 10
    POST_CONNECTION_AUTH = 20
    POST_CONNECTION_SCHEMA = 30
    POST_CONNECTION_DONE = 100


ctypedef object (*req_execute_func)(BaseProtocol, BaseRequest, float)

cdef class BaseProtocol(CoreProtocol):
    cdef:
        object loop
        str username
        str password
        bint fetch_schema
        bint auto_refetch_schema
        float request_timeout
        int post_con_state

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
        req_execute_func execute

        object create_future

    cdef void _set_connection_ready(self)
    cdef void _set_connection_error(self, e)
    cdef void _post_con_state_machine(self)

    cdef void _do_id(self)
    cdef void _do_auth(self, str username, str password)
    cdef void _do_fetch_schema(self, object fut)
    cdef object _refetch_schema(self)

    cdef inline uint64_t next_sync(self)
    cdef inline uint64_t next_stream_id(self)
    cdef uint32_t transform_iterator(self, iterator) except *

    cdef object _new_waiter_for_request(self, Response response, BaseRequest req, float timeout)
    cdef Db _create_db(self, bint gen_stream_id)
    cdef object _execute_bad(self, BaseRequest req, float timeout)
    cdef object _execute_normal(self, BaseRequest req, float timeout)
