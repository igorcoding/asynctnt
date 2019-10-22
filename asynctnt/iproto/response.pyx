import asyncio
import collections
from typing import Optional

cimport cpython
cimport cpython.list
cimport cpython.dict

from libc.stdint cimport uint32_t
from libc cimport stdio

from asynctnt.log import logger


@cython.final
@cython.freelist(REQUEST_FREELIST)
cdef class Response:
    """
        Response object for all the requests to Tarantool
    """

    def __cinit__(self, bytes encoding, bint push_subscribe):
        self._sync = 0
        self._code = -1
        self._return_code = -1
        self._schema_id = -1
        self._errmsg = None
        self._rowcount = 0
        self._body = None
        self._encoding = encoding
        self._fields = None
        self._autoincrement_ids = None
        self._push_subscribe = push_subscribe
        if push_subscribe:
            self._q = collections.deque()
            self._push_event = asyncio.Event()

            self._q_append = self._q.append
            self._q_popleft = self._q.popleft
            self._push_event_set = self._push_event.set
            self._push_event_clear = self._push_event.clear
        else:
            self._q = None
            self._push_event = None

    cdef inline bint is_error(self):
        return self._code >= 0x8000

    cdef inline void add_push(self, push):
        if not self._push_subscribe:
            return

        self._q_append(push)
        self.notify()

    cdef inline object push_len(self):
        return len(self._q)

    cdef inline object pop_push(self):
        if not self._push_subscribe:
            raise RuntimeError('Cannot pop push from a non-async response')

        push = self._q_popleft()
        if len(self._q) == 0:
            self._push_event_clear()
        return push

    cdef inline void set_data(self, list data):
        self._body = data
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
        data = self._body
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
        return self._sync

    @property
    def code(self) -> int:
        """
            Response code (0 - success)
        """
        return self._code

    @property
    def return_code(self) -> int:
        """
            Response return code (It's essentially a code & 0x7FFF)
        """
        return self._return_code

    @property
    def schema_id(self) -> int:
        """
            Current scema id in Tarantool
        """
        return self._schema_id

    @property
    def errmsg(self) -> Optional[str]:
        """
            If self.code != 0 then errmsg contains an error message
        """
        return self._errmsg

    @property
    def body(self) -> Optional[list]:
        """
            Response body
        """
        return self._body

    @property
    def encoding(self) -> str:
        """
            Response encoding
        """
        return self._encoding

    @property
    def rowcount(self) -> int:
        if self._body is not None:
            self_len = self._len()
            if self_len > 0:
                return self_len
        return self._rowcount

    @property
    def autoincrement_ids(self) -> Optional[list]:
        """
            Response autoincrement ids
        """
        return self._autoincrement_ids


    def done(self):
        return self._code >= 0

    cdef inline uint32_t _len(self):
        return <uint32_t>cpython.list.PyList_GET_SIZE(self._body)

    def __len__(self) -> int:
        if self._body is not None:
            return <int>self._len()
        return 0

    def __getitem__(self, i):
        return self._body[i]

    def __iter__(self):
        return iter(self._body)


cdef object _decode_obj(const char **p, bytes encoding):
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
            return <bytes>s[:s_len]
    elif obj_type == MP_BIN:
        s = NULL
        s_len = 0
        s = mp_decode_bin(p, &s_len)
        return <bytes>s[:s_len]
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
                map_key = <object>(
                    decode_string(map_key_str[:map_key_len], encoding)
                )
            elif map_key_type == MP_UINT:
                map_key = <object>mp_decode_uint(p)
            elif map_key_type == MP_INT:
                map_key = <object>mp_decode_int(p)
            elif map_key_type == MP_FLOAT:
                map_key = <object>mp_decode_float(p)
            elif map_key_type == MP_DOUBLE:
                map_key = <object>mp_decode_double(p)
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
    else:  # pragma: nocover
        mp_next(p)
        logger.warning('Unexpected obj type: %s', obj_type)
        return None


cdef list _response_parse_body_data(const char **b,
                                    Response resp, Request req):
    cdef:
        uint32_t size
        uint32_t tuple_size
        list tuples
        uint32_t i

        TntFields fields

    size = mp_decode_array(b)
    tuples = []

    if req.parse_as_tuples:
        # decode as TarantoolTuples

        fields = resp._fields
        if fields is None:
            fields = req.fields()

        for i in range(size):
            if mp_typeof(b[0][0]) != MP_ARRAY:  # pragma: nocover
                raise TypeError(
                    'Tuple must be an array when decoding as TarantoolTuple'
                )

            tuple_size = mp_decode_array(b)
            t = tupleobj.AtntTuple_New(fields, <int>tuple_size)
            for i in range(tuple_size):
                value = _decode_obj(b, resp._encoding)
                cpython.Py_INCREF(value)
                tupleobj.AtntTuple_SET_ITEM(t, i, value)

            tuples.append(t)
    else:
        # decode as raw objects
        for i in range(size):
            tuples.append(_decode_obj(b, resp._encoding))

    return tuples


cdef ssize_t response_parse_header(const char *buf, uint32_t buf_len,
                                   Header *hdr) except -1:
    cdef:
        const char *b
        uint32_t size
        uint32_t key

    b = <const char*>buf
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

            hdr.code = <uint32_t>mp_decode_uint(&b)
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

    return <ssize_t>(b - buf)


cdef ssize_t response_parse_body(const char *buf, uint32_t buf_len,
                                 Response resp, Request req,
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
        str field_name, field_type

    b = <const char*>buf
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
        if key == tarantool.IPROTO_ERROR:
            if mp_typeof(b[0]) != MP_STR:  # pragma: nocover
                raise TypeError('errstr type must be a MP_STR')

            s = NULL
            s_len = 0
            s = mp_decode_str(&b, &s_len)
            resp._errmsg = decode_string(s[:s_len], resp._encoding)
        elif key == tarantool.IPROTO_METADATA:
            if not req.parse_metadata:
                mp_next(&b)
                continue

            resp._fields = TntFields.__new__(TntFields)

            arr_size = mp_decode_array(&b)
            for i in range(arr_size):
                field_map_size = mp_decode_map(&b)
                if field_map_size == 0:
                    raise RuntimeError('Field map must contain at least '
                                       '1 element - field_name')

                field_id = i
                field_name = None
                field_type = None
                for _ in range(field_map_size):
                    key = mp_decode_uint(&b)
                    if key == tarantool.IPROTO_FIELD_NAME:
                        s = NULL
                        s_len = 0
                        s = mp_decode_str(&b, &s_len)
                        field_name = \
                            decode_string(s[:s_len], resp._encoding)
                    elif key == tarantool.IPROTO_FIELD_TYPE:
                        s = NULL
                        s_len = 0
                        s = mp_decode_str(&b, &s_len)
                        field_type = \
                            decode_string(s[:s_len], resp._encoding)
                    else:
                        logger.warning(
                            'unknown key in metadata decoding: %d', key)
                        mp_next(&b)

                if field_name is None:
                    raise RuntimeError('field_name must not be None')

                resp._fields.add(field_id, field_name)

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
                        el = <object>mp_decode_uint(&b)
                        cpython.Py_INCREF(el)
                        cpython.list.PyList_SET_ITEM(ids, i, el)
                    resp._autoincrement_ids = ids
                else:
                    logger.warning('unknown key in sql info decoding: %d', key)
                    mp_next(&b)

        elif key == tarantool.IPROTO_DATA:
            if mp_typeof(b[0]) != MP_ARRAY:  # pragma: nocover
                raise TypeError('body data type must be a MP_ARRAY')
            data = _response_parse_body_data(&b, resp, req)
            if is_chunk:
                resp.add_push(data)
            else:
                resp.set_data(data)

        else:
            logger.warning('unknown key in body map: %d', int(key))
            mp_next(&b)

    return <ssize_t>(b - buf)
