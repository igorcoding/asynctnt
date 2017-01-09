from libc.stdint cimport uint32_t, uint64_t, int64_t

cimport cmsgpuck as mp
cimport const as tnt_const

from response cimport TntResponse


cdef _decode_obj(const char** p):
    cdef:
        uint32_t i
        mp.mp_type obj_type
        
        const char* s
        uint32_t s_len
        
        uint32_t arr_size
        list arr
        
        uint32_t map_size
        dict map
        mp.mp_type map_key_type
        const char* map_key_str
        uint32_t map_key_len
        object map_key
        
        
    obj_type = mp.mp_typeof(p[0][0])
    if obj_type == mp.MP_UINT:
        return mp.mp_decode_uint(p)
    elif obj_type == mp.MP_INT:
        return mp.mp_decode_int(p)
    elif obj_type == mp.MP_STR:
        s = NULL
        s_len = 0
        s = mp.mp_decode_str(p, &s_len)
        return s[:s_len].decode()
    elif obj_type == mp.MP_BIN:
        s = NULL
        s_len = 0
        s = mp.mp_decode_bin(p, &s_len)
        return <bytes>s
    elif obj_type == mp.MP_BOOL:
        return mp.mp_decode_bool(p)
    elif obj_type == mp.MP_FLOAT:
        return mp.mp_decode_float(p)
    elif obj_type == mp.MP_DOUBLE:
        return mp.mp_decode_double(p)
    elif obj_type == mp.MP_ARRAY:
        arr_size = mp.mp_decode_array(p)
        value = []
        for i in range(arr_size):
            value.append(_decode_obj(p))
        return value
    elif obj_type == mp.MP_MAP:
        map = {}
        map_size = mp.mp_decode_map(p)
        for i in range(map_size):
            map_key_type = mp.mp_typeof(p[0][0])
            if map_key_type == mp.MP_STR:
                map_key_len = 0
                map_key_str = mp.mp_decode_str(p, &map_key_len)
                map_key = <object>(map_key_str[:map_key_len].decode())
            elif map_key_type == mp.MP_UINT:
                map_key = <object>mp.mp_decode_uint(p)
            elif map_key_type == mp.MP_INT:
                map_key = <object>mp.mp_decode_int(p)
            else:
                mp.mp_next(p)  # skip current key
                mp.mp_next(p)  # skip value
                print('Unexpected key type in map: {}'.format(map_key_type))
                
            map[map_key] = _decode_obj(p)
        
        return map
    elif obj_type == mp.MP_NIL:
        mp.mp_next(p)
        return None
    else:
        print('Unexpected obj type: {}'.format(obj_type))
        mp.mp_next(p)
        return None

    
cdef _cresponse_parse_body_data(const char* b):
    cdef:
        uint32_t size
        uint32_t tuple_size
        list tuples
        uint32_t i, k
        
    size = mp.mp_decode_array(&b)
    tuples = []
    for i in range(size):
        tuple_size = mp.mp_decode_array(&b)
        t = []
        for k in range(tuple_size):
            t.append(_decode_obj(&b))
        tuples.append(t)
    
    return tuples
            

cdef cresponse_parse(bytes buf):
    cdef:
        const char* b
        uint32_t size
        uint32_t key
        uint32_t s_len
        const char* s
        TntResponse resp
    
    buf_len = len(buf)
    begin = <const char*>buf
    b = <const char*>buf
    # if mp.mp_typeof(b[0]) != mp.MP_MAP:
    #     raise TypeError('Response header must be a MP_MAP')
    
    resp = TntResponse()
    
    # parsing header
    size = mp.mp_decode_map(&b)
    for _ in range(size):
        # if mp.mp_typeof(b[0]) != mp.MP_UINT:
        #     raise TypeError('Header key must be a MP_UINT')
        
        key = mp.mp_decode_uint(&b)
        if key == tnt_const.TP_CODE:
            # if mp.mp_typeof(b[0]) != mp.MP_UINT:
            #     raise TypeError('code type must be a MP_UINT')
            
            resp.code = mp.mp_decode_uint(&b)
            resp.code &= 0x7FFF
        elif key == tnt_const.TP_SYNC:
            # if mp.mp_typeof(b[0]) != mp.MP_UINT:
            #     raise TypeError('sync type must be a MP_UINT')
            
            resp.sync = mp.mp_decode_uint(&b)
        elif key == tnt_const.TP_SCHEMA_ID:
            # if mp.mp_typeof(b[0]) != mp.MP_UINT:
            #     raise TypeError('schema_id type must be a MP_UINT')
            
            resp.schema_id = mp.mp_decode_uint(&b)
        else:
            print('Unknown argument in header. Skipping.')
            mp.mp_next(&b)
    
    
    # parsing body
    if b == &begin[buf_len]:
        # buffer exceeded
        return resp
    
    # if mp.mp_typeof(b[0]) != mp.MP_MAP:
    #     raise TypeError('Response body must be a MP_MAP')
    
    size = mp.mp_decode_map(&b)
    for _ in range(size):
        # if mp.mp_typeof(b[0]) != mp.MP_UINT:
        #     raise TypeError('Header key must be a MP_UINT')
        
        key = mp.mp_decode_uint(&b)
        if key == tnt_const.TP_ERROR:
            # if mp.mp_typeof(b[0]) != mp.MP_STR:
            #     raise TypeError('errstr type must be a MP_STR')
            
            s = NULL
            s_len = 0
            s = mp.mp_decode_str(&b, &s_len)
            resp.errmsg = s[:s_len].decode()
        elif key == tnt_const.TP_DATA:
            if mp.mp_typeof(b[0]) != mp.MP_ARRAY:
                raise TypeError('body data type must be a MP_ARRAY')
            resp.body = _cresponse_parse_body_data(b)
            
    return resp

    


cpdef response_parse(bytes buf):
    res = cresponse_parse(buf)
    return res
