from libc.stdint cimport uint64_t, uint32_t, int64_t, int32_t

cdef struct Header:
    int32_t code
    int32_t return_code
    uint64_t sync
    int64_t schema_id


cdef class Response:
    cdef:
        int32_t _code
        int32_t _return_code
        uint64_t _sync
        int64_t _schema_id
        str _errmsg
        int _rowcount
        list _body
        bytes _encoding
        TntFields _fields
        list _autoincrement_ids
        bint _push_subscribe
        object _exception

        readonly object _q
        readonly object _push_event
        object _q_append
        object _q_popleft
        object _push_event_set
        object _push_event_clear

    cdef inline bint is_error(self)
    cdef inline uint32_t _len(self)
    cdef inline void add_push(self, push)
    cdef inline object pop_push(self)
    cdef inline object push_len(self)
    cdef inline void set_data(self, list data)
    cdef inline void set_exception(self, exc)
    cdef inline object get_exception(self)
    cdef inline void notify(self)


cdef ssize_t response_parse_header(const char *buf, uint32_t buf_len,
                                   Header *hdr) except -1
cdef ssize_t response_parse_body(const char *buf, uint32_t buf_len,
                                 Response resp, Request req,
                                 bint is_chunk) except -1
