import asyncio
import collections
from typing import Optional

cimport cpython
cimport cpython.dict
cimport cpython.list
cimport cython
from libc cimport stdio
from libc.stdint cimport uint32_t

from asynctnt.log import logger


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

@cython.final
@cython.freelist(REQUEST_FREELIST)
cdef class Response:
    """
        Response object for all the requests to Tarantool
    """

    def __cinit__(self):
        self.request_ = None
        self.sync_ = 0
        self.code_ = -1
        self.return_code_ = -1
        self.schema_id_ = -1
        self.errmsg = None
        self.error = None
        self._rowcount = 0
        self.body = None
        self.encoding = None
        self.metadata = None
        self.params = None
        self.params_count = 0
        self.autoincrement_ids = None
        self.stmt_id_ = 0

        self._push_subscribe = False
        self._q = None
        self._push_event = None
        self._q_append = None
        self._q_popleft = None
        self._push_event_set = None
        self._push_event_clear = None

    cdef inline bint is_error(self):
        return self.code_ >= 0x8000

    # noinspection PyAttributeOutsideInit
    cdef inline void init_push(self):
        self._push_subscribe = True
        self._q = collections.deque()
        self._push_event = asyncio.Event()

        self._q_append = self._q.append
        self._q_popleft = self._q.popleft
        self._push_event_set = self._push_event.set
        self._push_event_clear = self._push_event.clear

    cdef inline void add_push(self, push):
        if not self._push_subscribe:
            return

        self._q_append(push)
        self.notify()

    cdef inline int push_len(self):
        return len(self._q)

    cdef inline object pop_push(self):
        if not self._push_subscribe:
            raise RuntimeError('Cannot pop push from a non-async response')

        push = self._q_popleft()
        if len(self._q) == 0:
            self._push_event_clear()
        return push

    cdef inline void set_data(self, list data):
        self.body = data
        self.notify()

    cdef inline void set_exception(self, exc):
        self._exception = exc
        self.notify()

    cdef inline object get_exception(self):
        return self._exception

    cdef inline void notify(self):
        if self._push_subscribe:
            self._push_event_set()  # Notify that there is no more data

    def __repr__(self):  # pragma: nocover
        data = self.body
        if data is not None:
            if len(data) > 10:
                parts = map(lambda x: ', '.join(map(repr, x)), [
                    data[:5],
                    data[-2:]
                ])

                data = ' ... '.join(parts)
                data = '[' + data + ']'

        return '<{} sync={} rowcount={} data={}>'.format(
            self.__class__.__name__, self.sync, self.rowcount, data)

    @property
    def sync(self) -> int:
        """
            Response's sync (incremental id) for the corresponding request
        """
        return <int> self.sync_

    @property
    def code(self) -> int:
        """
            Response code (0 - success)
        """
        return <int> self.code_

    @property
    def return_code(self) -> int:
        """
            Response return code (It's essentially a code & 0x7FFF)
        """
        return <int> self.return_code_

    @property
    def schema_id(self) -> int:
        return <int> self.schema_id_

    @property
    def rowcount(self) -> int:
        if self.body is not None:
            self_len = self._len()
            if self_len > 0:
                return self_len
        return self._rowcount

    @property
    def stmt_id(self) -> Optional[int]:
        return self.stmt_id_

    def done(self):
        return self.code_ >= 0

    cdef inline uint32_t _len(self):
        return <uint32_t> cpython.list.PyList_GET_SIZE(self.body)

    def __len__(self) -> int:
        if self.body is not None:
            return <int> self._len()
        return 0

    def __getitem__(self, i):
        return self.body[i]

    def __iter__(self):
        return iter(self.body)

cdef object _decode_obj(const char ** p, bytes encoding):
    cdef:
        uint32_t i
        mp_type obj_type

        const char *s
        uint32_t s_len

        uint32_t arr_size
        list arr

        uint32_t map_size
        dict map
        mp_type map_key_type
        const char *map_key_str
        uint32_t map_key_len
        object map_key

        int8_t ext_type
        IProtoDateTime dt

    obj_type = mp_typeof(p[0][0])
    if obj_type == MP_UINT:
        return mp_decode_uint(p)
    elif obj_type == MP_INT:
        return mp_decode_int(p)
    elif obj_type == MP_STR:
        s = NULL
        s_len = 0
        s = mp_decode_str(p, &s_len)
        try:
            return decode_string(s[:s_len], encoding)
        except UnicodeDecodeError:
            return <bytes> s[:s_len]
    elif obj_type == MP_BIN:
        s = NULL
        s_len = 0
        s = mp_decode_bin(p, &s_len)
        return <bytes> s[:s_len]
    elif obj_type == MP_BOOL:
        return mp_decode_bool(p)
    elif obj_type == MP_FLOAT:
        return mp_decode_float(p)
    elif obj_type == MP_DOUBLE:
        return mp_decode_double(p)
    elif obj_type == MP_ARRAY:
        arr_size = mp_decode_array(p)
        value = cpython.list.PyList_New(arr_size)
        for i in range(arr_size):
            el = _decode_obj(p, encoding)
            cpython.Py_INCREF(el)
            cpython.list.PyList_SET_ITEM(value, i, el)
        return value
    elif obj_type == MP_MAP:
        map = {}
        map_size = mp_decode_map(p)
        for i in range(map_size):
            map_key_type = mp_typeof(p[0][0])
            if map_key_type == MP_STR:
                map_key_len = 0
                map_key_str = mp_decode_str(p, &map_key_len)
                map_key = <object> (
                    decode_string(map_key_str[:map_key_len], encoding)
                )
            elif map_key_type == MP_UINT:
                map_key = <object> mp_decode_uint(p)
            elif map_key_type == MP_INT:
                map_key = <object> mp_decode_int(p)
            elif map_key_type == MP_FLOAT:
                map_key = <object> mp_decode_float(p)
            elif map_key_type == MP_DOUBLE:
                map_key = <object> mp_decode_double(p)
            else:  # pragma: nocover
                mp_next(p)  # skip current key
                mp_next(p)  # skip value
                logger.warning('Unexpected key type in map: %s',
                               map_key_type)
                continue

            map[map_key] = _decode_obj(p, encoding)

        return map
    elif obj_type == MP_NIL:
        mp_next(p)
        return None
    elif obj_type == MP_EXT:
        ext_type = 0
        s_len = mp_decode_extl(p, &ext_type)
        if ext_type == tarantool.MP_DECIMAL:
            return decimal_decode(p, s_len)
        elif ext_type == tarantool.MP_UUID:
            return uuid_decode(p, s_len)
        elif ext_type == tarantool.MP_ERROR:
            return parse_iproto_error(p, encoding)
        elif ext_type == tarantool.MP_DATETIME:
            datetime_zero(&dt)
            datetime_decode(p, s_len, &dt)
            return datetime_to_py(&dt)
        else:  # pragma: nocover
            logger.warning('Unexpected ext type: %d', ext_type)
            p += s_len  # skip unknown ext
            return None
    else:  # pragma: nocover
        mp_next(p)
        logger.warning('Unexpected obj type: %s', obj_type)
        return None

cdef list _response_parse_body_data(const char ** b,
                                    Response resp, BaseRequest req):
    cdef:
        uint32_t size
        uint32_t tuple_size
        list tuples
        uint32_t i

        Metadata metadata

    size = mp_decode_array(b)
    tuples = []

    if req.parse_as_tuples:
        # decode as TarantoolTuples

        metadata = resp.metadata
        if metadata is None:
            metadata = req.metadata()

        for i in range(size):
            if mp_typeof(b[0][0]) != MP_ARRAY:  # pragma: nocover
                raise TypeError(
                    'Tuple must be an array when decoding as TarantoolTuple'
                )

            tuple_size = mp_decode_array(b)
            t = tupleobj.AtntTuple_New(metadata, <int> tuple_size)
            for i in range(tuple_size):
                value = _decode_obj(b, resp.encoding)
                cpython.Py_INCREF(value)
                tupleobj.AtntTuple_SET_ITEM(t, i, value)

            tuples.append(t)
    else:
        # decode as raw objects
        for i in range(size):
            tuples.append(_decode_obj(b, resp.encoding))

    return tuples

cdef ssize_t response_parse_header(const char *buf, uint32_t buf_len,
                                   Header *hdr) except -1:
    cdef:
        const char *b
        uint32_t size
        uint32_t key

    b = <const char *> buf
    # mp_fprint(stdio.stdout, b)
    # stdio.fprintf(stdio.stdout, "\n")

    if mp_typeof(b[0]) != MP_MAP:  # pragma: nocover
        raise TypeError('Response header must be a MP_MAP')

    # parsing header
    size = mp_decode_map(&b)
    for _ in range(size):
        if mp_typeof(b[0]) != MP_UINT:  # pragma: nocover
            raise TypeError('Header key must be a MP_UINT')

        key = mp_decode_uint(&b)
        if key == tarantool.IPROTO_REQUEST_TYPE:
            if mp_typeof(b[0]) != MP_UINT:  # pragma: nocover
                raise TypeError('code type must be a MP_UINT')

            hdr.code = <uint32_t> mp_decode_uint(&b)
            hdr.return_code = hdr.code & 0x7FFF
        elif key == tarantool.IPROTO_SYNC:
            if mp_typeof(b[0]) != MP_UINT:  # pragma: nocover
                raise TypeError('sync type must be a MP_UINT')

            hdr.sync = mp_decode_uint(&b)
        elif key == tarantool.IPROTO_SCHEMA_VERSION:
            if mp_typeof(b[0]) != MP_UINT:  # pragma: nocover
                raise TypeError('schema_id type must be a MP_UINT')

            hdr.schema_id = mp_decode_uint(&b)
        else:  # pragma: nocover
            logger.warning(
                'Unknown key with code \'%d\' in header. Skipping.', key)
            mp_next(&b)

    return <ssize_t> (b - buf)

cdef Metadata response_parse_metadata(const char ** b, bytes encoding):
    cdef:
        uint32_t arr_size
        uint32_t field_map_size
        uint32_t key
        uint32_t s_len
        uint32_t i
        const char *s
        Field field
        Metadata metadata

    metadata = <Metadata> Metadata.__new__(Metadata)
    arr_size = mp_decode_array(b)
    for i in range(arr_size):
        field_map_size = mp_decode_map(b)
        if field_map_size == 0:  # pragma: nocover
            raise RuntimeError('Field map must contain at least '
                               '1 element - field_name')

        field = <Field> Field.__new__(Field)
        field_id = i
        for _ in range(field_map_size):
            key = mp_decode_uint(b)
            if key == tarantool.IPROTO_FIELD_NAME:
                s = NULL
                s_len = 0
                s = mp_decode_str(b, &s_len)
                field.name = \
                    decode_string(s[:s_len], encoding)

            elif key == tarantool.IPROTO_FIELD_TYPE:
                s = NULL
                s_len = 0
                s = mp_decode_str(b, &s_len)
                field.type = \
                    decode_string(s[:s_len], encoding)

            elif key == tarantool.IPROTO_FIELD_COLL:
                s = NULL
                s_len = 0
                s = mp_decode_str(b, &s_len)
                field.collation = \
                    decode_string(s[:s_len], encoding)

            elif key == tarantool.IPROTO_FIELD_IS_NULLABLE:
                field.is_nullable = mp_decode_bool(b)

            elif key == tarantool.IPROTO_FIELD_IS_AUTOINCREMENT:
                field.is_autoincrement = mp_decode_bool(b)

            elif key == tarantool.IPROTO_FIELD_SPAN:
                if mp_typeof(b[0][0]) == MP_NIL:  # pragma: nocover
                    mp_next(b)
                    field.span = None

                elif mp_typeof(b[0][0]) == MP_STR:
                    s = NULL
                    s_len = 0
                    s = mp_decode_str(b, &s_len)
                    field.span = \
                        decode_string(s[:s_len], encoding)

                else:  # pragma: nocover
                    raise TypeError(
                        "IPROTO_FIELD_SPAN must be either STR or NIL"
                    )
            else:  # pragma: nocover
                logger.debug(
                    'unknown key in metadata decoding: %d', key)
                mp_next(b)

        if field.name is None:  # pragma: nocover
            raise RuntimeError('field.name must not be None')

        metadata.add(<int> field_id, field)
    return metadata

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

cdef inline IProtoError parse_iproto_error(const char ** b, bytes encoding):
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

cdef ssize_t response_parse_body(const char *buf, uint32_t buf_len,
                                 Response resp, BaseRequest req,
                                 bint is_chunk) except -1:
    cdef:
        const char *b
        uint32_t size
        uint32_t arr_size
        uint32_t field_map_size
        uint32_t key
        uint32_t s_len
        uint32_t i
        const char *s
        list data
        Field field

    b = <const char *> buf
    # mp_fprint(stdio.stdout, b)
    # stdio.fprintf(stdio.stdout, "\n")

    # parsing body

    if mp_typeof(b[0]) != MP_MAP:  # pragma: nocover
        raise TypeError('Response body must be a MP_MAP')

    size = mp_decode_map(&b)
    for _ in range(size):
        if mp_typeof(b[0]) != MP_UINT:  # pragma: nocover
            raise TypeError('Header key must be a MP_UINT')

        key = mp_decode_uint(&b)
        if key == tarantool.IPROTO_ERROR_24:
            if mp_typeof(b[0]) != MP_STR:  # pragma: nocover
                raise TypeError('errstr type must be a MP_STR')

            s = NULL
            s_len = 0
            s = mp_decode_str(&b, &s_len)
            resp.errmsg = decode_string(s[:s_len], resp.encoding)

        elif key == tarantool.IPROTO_ERROR:
            if mp_typeof(b[0]) != MP_MAP:  # pragma: nocover
                raise TypeError('IPROTO_ERROR type must be a MP_MAP')

            resp.error = parse_iproto_error(&b, resp.encoding)

        elif key == tarantool.IPROTO_STMT_ID:
            if mp_typeof(b[0]) != MP_UINT:  # pragma: nocover
                raise TypeError(f'IPROTO_STMT_ID type must be a MP_UINT, but got {mp_typeof(b[0])}')
            resp.stmt_id_ = mp_decode_uint(&b)

        elif key == tarantool.IPROTO_METADATA:
            if not req.parse_metadata:
                mp_next(&b)
                continue

            resp.metadata = response_parse_metadata(&b, resp.encoding)

        elif key == tarantool.IPROTO_BIND_METADATA:
            if not req.parse_metadata:
                mp_next(&b)
                continue

            resp.params = response_parse_metadata(&b, resp.encoding)

        elif key == tarantool.IPROTO_BIND_COUNT:

            resp.params_count = <int> mp_decode_uint(&b)

        elif key == tarantool.IPROTO_SQL_INFO:
            field_map_size = mp_decode_map(&b)
            if field_map_size == 0:
                raise RuntimeError('Field map must contain at least '
                                   '1 element - rowcount')

            for _ in range(field_map_size):
                key = mp_decode_uint(&b)
                if key == tarantool.SQL_INFO_ROW_COUNT:
                    resp._rowcount = mp_decode_uint(&b)
                elif key == tarantool.SQL_INFO_AUTOINCREMENT_IDS:
                    arr_size = mp_decode_array(&b)
                    ids = cpython.list.PyList_New(arr_size)
                    for i in range(arr_size):
                        el = <object> mp_decode_uint(&b)
                        cpython.Py_INCREF(el)
                        cpython.list.PyList_SET_ITEM(ids, i, el)
                    resp.autoincrement_ids = ids
                else:
                    logger.debug('unknown key in sql info decoding: %d', key)
                    mp_next(&b)

        elif key == tarantool.IPROTO_DATA:
            if mp_typeof(b[0]) != MP_ARRAY:  # pragma: nocover
                raise TypeError('body data type must be a MP_ARRAY')
            data = _response_parse_body_data(&b, resp, req)
            if is_chunk:
                resp.add_push(data)
            else:
                resp.set_data(data)

        elif key == tarantool.IPROTO_VERSION:
            logger.debug("IProto version: %s", _decode_obj(&b, resp.encoding))

        elif key == tarantool.IPROTO_FEATURES:
            logger.debug("IProto features available: %s", _decode_obj(&b, resp.encoding))

        elif key == tarantool.IPROTO_AUTH_TYPE:
            logger.debug("IProto auth type: %s", _decode_obj(&b, resp.encoding))

        else:  # pragma: nocover
            logger.debug('unknown key in body map: %s', hex(int(key)))
            mp_next(&b)

    return <ssize_t> (b - buf)
