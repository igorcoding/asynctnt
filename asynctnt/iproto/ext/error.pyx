cimport cpython.list
cimport cython
from libc.stdint cimport uint32_t


@cython.final
cdef class IProtoErrorStackFrame:
    def __repr__(self):
        return "<Frame type={} code={} message={}>".format(
            self.error_type,
            self.code,
            self.message,
        )

@cython.final
cdef class IProtoError:
    pass

cdef inline IProtoErrorStackFrame parse_iproto_error_stack_frame(const char ** b, bytes encoding):
    cdef:
        uint32_t size
        uint32_t key
        const char * s
        uint32_t s_len
        IProtoErrorStackFrame frame
        uint32_t unum

    size = 0
    key = 0

    frame = <IProtoErrorStackFrame> IProtoErrorStackFrame.__new__(IProtoErrorStackFrame)

    size = mp_decode_map(b)
    for _ in range(size):
        key = mp_decode_uint(b)

        if key == tarantool.MP_ERROR_TYPE:
            s = NULL
            s_len = 0
            s = mp_decode_str(b, &s_len)
            frame.error_type = decode_string(s[:s_len], encoding)

        elif key == tarantool.MP_ERROR_FILE:
            s = NULL
            s_len = 0
            s = mp_decode_str(b, &s_len)
            frame.file = decode_string(s[:s_len], encoding)

        elif key == tarantool.MP_ERROR_LINE:
            frame.line = <int> mp_decode_uint(b)

        elif key == tarantool.MP_ERROR_MESSAGE:
            s = NULL
            s_len = 0
            s = mp_decode_str(b, &s_len)
            frame.message = decode_string(s[:s_len], encoding)

        elif key == tarantool.MP_ERROR_ERRNO:
            frame.err_no = <int> mp_decode_uint(b)

        elif key == tarantool.MP_ERROR_ERRCODE:
            frame.code = <int> mp_decode_uint(b)

        elif key == tarantool.MP_ERROR_FIELDS:
            if mp_typeof(b[0][0]) != MP_MAP:  # pragma: nocover
                raise TypeError(f'iproto_error stack frame fields must be a '
                                f'map, but got {mp_typeof(b[0][0])}')

            frame.fields = _decode_obj(b, encoding)

        else:  # pragma: nocover
            logger.debug(f"unknown iproto_error stack element with key {key}")
            mp_next(b)

    return frame

cdef inline IProtoError iproto_error_decode(const char ** b, bytes encoding):
    cdef:
        uint32_t size
        uint32_t arr_size
        uint32_t key
        uint32_t i
        IProtoError error

    size = 0
    arr_size = 0
    key = 0

    error = <IProtoError> IProtoError.__new__(IProtoError)

    size = mp_decode_map(b)
    for _ in range(size):
        key = mp_decode_uint(b)

        if key == tarantool.MP_ERROR_STACK:
            arr_size = mp_decode_array(b)
            error.trace = cpython.list.PyList_New(arr_size)
            for i in range(arr_size):
                el = parse_iproto_error_stack_frame(b, encoding)
                cpython.Py_INCREF(el)
                cpython.list.PyList_SET_ITEM(error.trace, i, el)
        else:  # pragma: nocover
            logger.debug(f"unknown iproto_error map field with key {key}")
            mp_next(b)

    return error
