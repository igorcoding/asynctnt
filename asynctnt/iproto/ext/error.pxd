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

cdef IProtoError iproto_error_decode(const char ** b, bytes encoding)
