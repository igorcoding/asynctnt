include "const.pxi"

include "cmsgpuck.pxd"
include "python.pxd"

include "unicode.pxd"
include "buffer.pxd"
include "rbuffer.pxd"
include "request.pxd"
include "response.pxd"
include "schema.pxd"
include "db.pxd"

include "coreproto.pxd"


cdef class BaseProtocol(CoreProtocol):
    cdef:
        object loop
        str username
        str password
        bint fetch_schema
        bint auto_refetch_schema
        bint tuple_as_dict
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
        Db _db

        object create_future

    cdef void _set_connection_ready(self)
    cdef void _set_connection_error(self, e)

    cdef void _do_auth(self, str username, str password)
    cdef object _do_fetch_schema(self)

    cdef uint64_t next_sync(self)
    cdef uint32_t transform_iterator(self, iterator) except *

    cdef object _new_waiter_for_request(self, Request req, float timeout)
    cdef Db _create_db(self)
    cdef object execute(self, Request req, float timeout)
