cimport cpython.unicode

from libc.stdint cimport uint32_t, uint64_t, int64_t

cimport tnt

from asynctnt.log import logger
import yaml


cdef class Response:
    @staticmethod
    cdef inline Response new(bytes encoding):
        cdef Response resp
        resp = Response.__new__(Response)
        resp.sync = 0
        resp.code = 0
        resp.schema_id = -1
        resp.errmsg = None
        resp.body = None
        resp.encoding = encoding
        return resp

    cdef inline has_schema_id(self):
        return self.schema_id != -1

    cdef inline is_error(self):
        return self.code != 0

    def __repr__(self):
        return '<Response: code={}, sync={}>'.format(self.code, self.sync)

    def body2yaml(self):
        return yaml.dump(self.body, allow_unicode=True)


cdef object _decode_obj(const char** p, bytes encoding):
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
        return s[:s_len].decode(encoding)
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
                map_key = <object>(map_key_str[:map_key_len].decode(encoding))
            elif map_key_type == MP_UINT:
                map_key = <object>mp_decode_uint(p)
            elif map_key_type == MP_INT:
                map_key = <object>mp_decode_int(p)
            else:
                mp_next(p)  # skip current key
                mp_next(p)  # skip value
                logger.warning('Unexpected key type in map: %s',
                               map_key_type)

            map[map_key] = _decode_obj(p, encoding)

        return map
    elif obj_type == MP_NIL:
        mp_next(p)
        return None
    else:
        mp_next(p)
        logger.warning('Unexpected obj type: %s', obj_type)
        return None


cdef list _response_parse_body_data(const char *b, bytes encoding):
    cdef:
        uint32_t size
        uint32_t tuple_size
        list tuples
        uint32_t i, k

    size = mp_decode_array(&b)
    tuples = []
    for i in range(size):
        tuples.append(_decode_obj(&b, encoding))

    return tuples


cdef Response response_parse(const char *buf, uint32_t buf_len,
                             bytes encoding):
    cdef:
        const char *b
        uint32_t size
        uint32_t key
        uint32_t s_len
        const char *s
        Response resp

    b = <const char*>buf
    if mp_typeof(b[0]) != MP_MAP:
        raise TypeError('Response header must be a MP_MAP')

    resp = Response.new(encoding)

    # parsing header
    size = mp_decode_map(&b)
    for _ in range(size):
        if mp_typeof(b[0]) != MP_UINT:
            raise TypeError('Header key must be a MP_UINT')

        key = mp_decode_uint(&b)
        if key == tnt.TP_CODE:
            if mp_typeof(b[0]) != MP_UINT:
                raise TypeError('code type must be a MP_UINT')

            resp.code = <uint32_t>mp_decode_uint(&b)
            resp.code &= 0x7FFF
        elif key == tnt.TP_SYNC:
            if mp_typeof(b[0]) != MP_UINT:
                raise TypeError('sync type must be a MP_UINT')

            resp.sync = mp_decode_uint(&b)
        elif key == tnt.TP_SCHEMA_ID:
            if mp_typeof(b[0]) != MP_UINT:
                raise TypeError('schema_id type must be a MP_UINT')

            resp.schema_id = mp_decode_uint(&b)
        else:
            logger.warning('Unknown argument in header. Skipping.')
            mp_next(&b)

    # parsing body
    if b == &buf[buf_len]:
        # buffer exceeded
        return resp

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
            resp.errmsg = s[:s_len].decode(encoding)
        elif key == tnt.TP_DATA:
            if mp_typeof(b[0]) != MP_ARRAY:
                raise TypeError('body data type must be a MP_ARRAY')
            resp.body = _response_parse_body_data(b, encoding)

    return resp
