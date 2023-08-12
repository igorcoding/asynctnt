cimport cython
from libc.stdint cimport int32_t, int64_t, uint32_t, uint64_t


cdef struct Header:
    int32_t code
    int32_t return_code
    uint64_t sync
    int64_t schema_id

cdef class IProtoErrorStackFrame:
    cdef:
        readonly str error_type
        readonly str file
        readonly int line
        readonly str message
        readonly int err_no
        readonly int code
        readonly dict fields

cdef class IProtoError:
    cdef:
        readonly list trace

cdef class Response:
    cdef:
        int32_t code_
        int32_t return_code_
        uint64_t sync_
        int64_t schema_id_
        readonly str errmsg
        readonly IProtoError error
        int _rowcount
        readonly list body
        readonly bytes encoding
        readonly Metadata metadata
        readonly Metadata params
        readonly int params_count
        readonly list autoincrement_ids
        uint64_t stmt_id_
        bint _push_subscribe
        BaseRequest request_
        object _exception

        readonly object _q
        readonly object _push_event
        object _q_append
        object _q_popleft
        object _push_event_set
        object _push_event_clear

    cdef inline bint is_error(self)
    cdef inline uint32_t _len(self)
    cdef inline void init_push(self)
    cdef inline void add_push(self, push)
    cdef inline object pop_push(self)
    cdef inline int push_len(self)
    cdef inline void set_data(self, list data)
    cdef inline void set_exception(self, exc)
    cdef inline object get_exception(self)
    cdef inline void notify(self)

cdef ssize_t response_parse_header(const char *buf, uint32_t buf_len,
                                   Header *hdr) except -1
cdef ssize_t response_parse_body(const char *buf, uint32_t buf_len,
                                 Response resp, BaseRequest req,
                                 bint is_chunk) except -1
cdef IProtoError parse_iproto_error(const char ** b, bytes encoding)
