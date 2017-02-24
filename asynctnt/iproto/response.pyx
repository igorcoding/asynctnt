cimport cpython.unicode
cimport cpython.list
cimport cpython.dict

from libc.stdint cimport uint32_t, uint64_t, int64_t

cimport tnt

from asynctnt.log import logger
import yaml


cdef class Response:
    def __cinit__(self):
        self._sync = 0
        self._code = 0
        self._schema_id = -1
        self._errmsg = None
        self._body = None
        self._encoding = None
        self._req = None

    @staticmethod
    cdef inline Response new(bytes encoding):
        cdef Response resp
        resp = Response.__new__(Response)
        resp._encoding = encoding
        return resp

    cdef inline is_error(self):
        return self._code != 0

    def __repr__(self):
        body_len = None
        if self._body is not None:
            body_len = len(self._body)
        return '<Response: code={}, sync={}, body_len={}>'.format(
            self._code, self._sync, body_len)

    def body2yaml(self):
        return yaml.dump(self._body, allow_unicode=True)

    @property
    def sync(self):
        return self._sync

    @property
    def code(self):
        return self._code

    @property
    def schema_id(self):
        return self._schema_id

    @property
    def errmsg(self):
        return self._errmsg

    @property
    def body(self):
        return self._body

    @property
    def encoding(self):
        return self._encoding


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
        return decode_string(s[:s_len], encoding)
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
        value = []
        for i in range(arr_size):
            value.append(_decode_obj(p, encoding))
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
            else:
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
    else:
        mp_next(p)
        logger.warning('Unexpected obj type: %s', obj_type)
        return None


cdef list _response_parse_body_data(const char **b, Response resp):
    cdef:
        uint32_t size
        uint32_t tuple_size, max_tuple_size
        list tuples
        uint32_t i, k

        Request req
        dict d
        str field_name
        object value
        list fields

    size = mp_decode_array(b)
    tuples = []
    req = resp._req
    if req is not None \
            and req.tuple_as_dict \
            and req.space is not None \
            and req.space.fields is not None:
        # decode as dicts

        fields = req.space.fields
        max_tuple_size = <uint32_t>cpython.list.PyList_GET_SIZE(fields)

        for i in range(size):
            if mp_typeof(b[0][0]) != MP_ARRAY:
                raise TypeError(
                    'Tuple must be an array when decoding as dict'
                )
            d = {}
            unknown_fields = None
            tuple_size = mp_decode_array(b)
            for k in range(tuple_size):
                value = _decode_obj(b, resp._encoding)
                if k < max_tuple_size:
                    field_name = <str>cpython.list.PyList_GET_ITEM(fields, k)
                    d[field_name] = value
                else:
                    if unknown_fields is None:
                        unknown_fields = []
                    unknown_fields.append(value)

            if unknown_fields is not None:
                d[''] = unknown_fields
            tuples.append(d)
    else:
        for i in range(size):
            tuples.append(_decode_obj(b, resp._encoding))

    return tuples


cdef ssize_t response_parse_header(const char *buf, uint32_t buf_len,
                                   Response resp) except -1:
    cdef:
        const char *b
        uint32_t size
        uint32_t key

    b = <const char*>buf
    if mp_typeof(b[0]) != MP_MAP:
        raise TypeError('Response header must be a MP_MAP')

    # parsing header
    size = mp_decode_map(&b)
    for _ in range(size):
        if mp_typeof(b[0]) != MP_UINT:
            raise TypeError('Header key must be a MP_UINT')

        key = mp_decode_uint(&b)
        if key == tnt.TP_CODE:
            if mp_typeof(b[0]) != MP_UINT:
                raise TypeError('code type must be a MP_UINT')

            resp._code = <uint32_t>mp_decode_uint(&b)
            resp._code &= 0x7FFF
        elif key == tnt.TP_SYNC:
            if mp_typeof(b[0]) != MP_UINT:
                raise TypeError('sync type must be a MP_UINT')

            resp._sync = mp_decode_uint(&b)
        elif key == tnt.TP_SCHEMA_ID:
            if mp_typeof(b[0]) != MP_UINT:
                raise TypeError('schema_id type must be a MP_UINT')

            resp._schema_id = mp_decode_uint(&b)
        else:
            logger.warning('Unknown argument in header. Skipping.')
            mp_next(&b)

    return <ssize_t>(b - buf)


cdef ssize_t response_parse_body(const char *buf, uint32_t buf_len,
                                 Response resp) except -1:
    cdef:
        const char *b
        uint32_t size
        uint32_t key
        uint32_t s_len
        const char *s

    b = <const char*>buf

    # parsing body
    if b == &buf[buf_len]:
        # buffer exceeded
        return 0

    if mp_typeof(b[0]) != MP_MAP:
        raise TypeError('Response body must be a MP_MAP')

    size = mp_decode_map(&b)
    for _ in range(size):
        if mp_typeof(b[0]) != MP_UINT:
            raise TypeError('Header key must be a MP_UINT')

        key = mp_decode_uint(&b)
        if key == tnt.TP_ERROR:
            if mp_typeof(b[0]) != MP_STR:
                raise TypeError('errstr type must be a MP_STR')

            s = NULL
            s_len = 0
            s = mp_decode_str(&b, &s_len)
            resp._errmsg = decode_string(s[:s_len], resp._encoding)
        elif key == tnt.TP_DATA:
            if mp_typeof(b[0]) != MP_ARRAY:
                raise TypeError('body data type must be a MP_ARRAY')
            resp._body = _response_parse_body_data(&b, resp)

    return <ssize_t>(b - buf)
