cimport cpython.datetime
from cpython.datetime cimport PyDateTimeAPI, datetime, datetime_tzinfo, timedelta_new
from libc.stdint cimport uint32_t
from libc.string cimport memcpy


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
