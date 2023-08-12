cimport cpython.datetime
from cpython.datetime cimport PyDateTimeAPI, datetime, datetime_tzinfo, timedelta_new
from libc.stdint cimport uint32_t
from libc.string cimport memcpy

from decimal import Decimal
from uuid import UUID


cdef uint32_t decimal_len(int exponent, uint32_t digits_count):
    cdef:
        uint32_t length

    length = bcd_len(digits_count)
    if exponent > 0:
        length += mp_sizeof_int(-exponent)
    else:
        length += mp_sizeof_uint(-exponent)

    return length

cdef char *decimal_encode(char *p,
                          uint32_t digits_count,
                          uint8_t sign,
                          tuple digits,
                          int exponent) except NULL:
    cdef:
        int i
        uint8_t byte
        char *out
        uint32_t length

    # encode exponent
    if exponent > 0:
        p = mp_encode_int(p, -exponent)
    else:
        p = mp_encode_uint(p, -exponent)

    length = bcd_len(digits_count)

    out = &p[length - 1]
    if sign == 1:
        byte = 0x0d
    else:
        byte = 0x0c

    i = digits_count - 1
    while out >= p:
        if i >= 0:
            byte |= (<uint8_t> <object> cpython.tuple.PyTuple_GET_ITEM(digits, i)) << 4

        out[0] = byte
        byte = 0

        if i > 0:
            byte = <uint8_t> <object> cpython.tuple.PyTuple_GET_ITEM(digits, i - 1) & 0xf

        out -= 1
        i -= 2

    p = &p[length]
    return p

cdef object decimal_decode(const char ** p, uint32_t length):
    cdef:
        int exponent
        uint8_t sign
        mp_type obj_type
        const char *first
        const char *last
        uint32_t digits_count
        uint8_t nibble

    sign = 0
    first = &p[0][0]
    last = first + length - 1

    # decode exponent
    obj_type = mp_typeof(p[0][0])
    if obj_type == MP_UINT:
        exponent = -<int> mp_decode_uint(p)
    elif obj_type == MP_INT:
        exponent = -<int> mp_decode_int(p)
    else:
        raise TypeError('unexpected exponent type: {}'.format(obj_type))

    length -= (&p[0][0] - first)
    first = &p[0][0]

    while first[0] == 0:
        first += 1  # skipping leading zeros

    sign = last[0] & 0xf  # extract sign
    if sign == 0x0a or sign == 0x0c or sign == 0x0e or sign == 0x0f:
        sign = 0
    else:
        sign = 1

    # decode digits
    digits_count = (last - first) * 2 + 1
    if first[0] & 0xf0 == 0:
        digits_count -= 1  # adjust for leading zero nibble

    digits = cpython.tuple.PyTuple_New(digits_count)

    if digits_count > 0:
        while True:
            nibble = (last[0] & 0xf0) >> 4  # left nibble first
            item = <object> <int> nibble
            cpython.Py_INCREF(item)
            cpython.tuple.PyTuple_SET_ITEM(digits, digits_count - 1, item)

            digits_count -= 1
            if digits_count == 0:
                break
            last -= 1

            nibble = last[0] & 0x0f  # right nibble
            item = <object> <int> nibble
            cpython.Py_INCREF(item)
            cpython.tuple.PyTuple_SET_ITEM(digits, digits_count - 1, item)

            digits_count -= 1
            if digits_count == 0:
                break

    p[0] += length

    return Decimal((<object> <int> sign, digits, <object> exponent))

cdef object uuid_decode(const char ** p, uint32_t length):
    data = cpython.bytes.PyBytes_FromStringAndSize(p[0], length)
    p[0] += length
    return UUID(bytes=data)

cdef inline void datetime_zero(IProtoDateTime *dt):
    dt.seconds = 0
    dt.nsec = 0
    dt.tzoffset = 0
    dt.tzindex = 0

cdef inline uint32_t datetime_len(IProtoDateTime *dt):
    cdef uint32_t sz
    sz = sizeof(int64_t)
    if dt.nsec != 0 or dt.tzoffset != 0 or dt.tzindex != 0:
        return sz + DATETIME_TAIL_SZ
    return sz

cdef char *datetime_encode(char *p, IProtoDateTime *dt) except NULL:
    store_u64(p, dt.seconds)
    p += sizeof(dt.seconds)
    if dt.nsec != 0 or dt.tzoffset != 0 or dt.tzindex != 0:
        memcpy(p, &dt.nsec, DATETIME_TAIL_SZ)
        p += DATETIME_TAIL_SZ
    return p

cdef int datetime_decode(
        const char ** p,
        uint32_t length,
        IProtoDateTime *dt
) except -1:
    delta = None
    tz = None

    dt.seconds = load_u64(p[0])
    p[0] += sizeof(dt.seconds)
    length -= sizeof(dt.seconds)

    if length == 0:
        return 0

    if length != DATETIME_TAIL_SZ:
        raise ValueError("invalid datetime size. got {} extra bytes".format(
            length
        ))

    dt.nsec = load_u32(p[0])
    p[0] += 4
    dt.tzoffset = load_u16(p[0])
    p[0] += 2
    dt.tzindex = load_u16(p[0])
    p[0] += 2

cdef void datetime_from_py(datetime ob, IProtoDateTime *dt):
    cdef:
        double ts
        int offset
    ts = <double> ob.timestamp()
    dt.seconds = <int64_t> ts
    dt.nsec = <int32_t> ((ts - <double> dt.seconds) * 1000000) * 1000
    if dt.nsec < 0:
        # correction for negative dates
        dt.seconds -= 1
        dt.nsec += 1000000000

    if datetime_tzinfo(ob) is not None:
        offset = ob.utcoffset().total_seconds()
        dt.tzoffset = <int16_t> (offset / 60)

cdef object datetime_to_py(IProtoDateTime *dt):
    cdef:
        double timestamp
        object tz

    tz = None

    if dt.tzoffset != 0:
        delta = timedelta_new(0, <int> dt.tzoffset * 60, 0)
        tz = timezone_new(delta)

    timestamp = dt.seconds + (<double> dt.nsec) / 1e9
    return PyDateTimeAPI.DateTime_FromTimestamp(
        <PyObject *>PyDateTimeAPI.DateTimeType,
        (timestamp,) if tz is None else (timestamp, tz),
        NULL,
    )
