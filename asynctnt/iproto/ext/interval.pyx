import enum

from libc.stdint cimport int64_t, uint8_t, uint32_t, uint64_t


class Adjust(enum.IntEnum):
    """
    Interval adjustment mode for year and month arithmetic.
    """
    EXCESS = 0
    NONE = 1
    LAST = 2


cdef class MPInterval:
    def __cinit__(self,
                  int year=0,
                  int month=0,
                  int week=0,
                  int day=0,
                  int hour=0,
                  int min=0,
                  int sec=0,
                  int nsec=0,
                  object adjust=Adjust.NONE):
        self.year = year
        self.month = month
        self.week = week
        self.day = day
        self.hour = hour
        self.min = min
        self.sec = sec
        self.nsec = nsec
        self.adjust = adjust

    def __repr__(self):
        return (f"asynctnt.Interval("
                f"year={self.year}, "
                f"month={self.month}, "
                f"week={self.week}, "
                f"day={self.day}, "
                f"hour={self.hour}, "
                f"min={self.min}, "
                f"sec={self.sec}, "
                f"nsec={self.nsec}, "
                f"adjust={self.adjust!r}"
                f")")

    def __eq__(self, other):
        cdef:
            MPInterval other_interval

        if not isinstance(other, MPInterval):
            return False

        other_interval = <MPInterval> other

        return (self.year == other_interval.year
                and self.month == other_interval.month
                and self.week == other_interval.week
                and self.day == other_interval.day
                and self.hour == other_interval.hour
                and self.min == other_interval.min
                and self.sec == other_interval.sec
                and self.nsec == other_interval.nsec
                and self.adjust == other_interval.adjust
                )

cdef uint32_t interval_value_len(int64_t value):
    if value == 0:
        return 0

    if value > 0:
        return 1 + mp_sizeof_uint(<uint64_t> value)

    return 1 + mp_sizeof_int(value)

cdef char *interval_value_pack(char *data, mp_interval_fields field, int64_t value):
    if value == 0:
        return data

    data = mp_encode_uint(data, field)

    if value > 0:
        return mp_encode_uint(data, <uint64_t> value)

    return mp_encode_int(data, value)

cdef uint32_t interval_len(MPInterval interval):
    return (1
            + interval_value_len(interval.year)
            + interval_value_len(interval.month)
            + interval_value_len(interval.week)
            + interval_value_len(interval.day)
            + interval_value_len(interval.hour)
            + interval_value_len(interval.min)
            + interval_value_len(interval.sec)
            + interval_value_len(interval.nsec)
            + interval_value_len(<int64_t> interval.adjust.value)
            )

cdef char *interval_encode(char *data, MPInterval interval) except NULL:
    cdef:
        uint8_t fields_count

    fields_count = (<uint8_t>(interval.year != 0)
                    + <uint8_t>(interval.month != 0)
                    + <uint8_t>(interval.week != 0)
                    + <uint8_t>(interval.day != 0)
                    + <uint8_t>(interval.hour != 0)
                    + <uint8_t>(interval.min != 0)
                    + <uint8_t>(interval.sec != 0)
                    + <uint8_t>(interval.nsec != 0)
                    + <uint8_t>(interval.adjust != 0)
                    )
    data = mp_store_u8(data, fields_count)
    data = interval_value_pack(data, MP_INTERVAL_FIELD_YEAR, <int64_t> interval.year)
    data = interval_value_pack(data, MP_INTERVAL_FIELD_MONTH, <int64_t> interval.month)
    data = interval_value_pack(data, MP_INTERVAL_FIELD_WEEK, <int64_t> interval.week)
    data = interval_value_pack(data, MP_INTERVAL_FIELD_DAY, <int64_t> interval.day)
    data = interval_value_pack(data, MP_INTERVAL_FIELD_HOUR, <int64_t> interval.hour)
    data = interval_value_pack(data, MP_INTERVAL_FIELD_MINUTE, <int64_t> interval.min)
    data = interval_value_pack(data, MP_INTERVAL_FIELD_SECOND, <int64_t> interval.sec)
    data = interval_value_pack(data, MP_INTERVAL_FIELD_NANOSECOND, <int64_t> interval.nsec)
    data = interval_value_pack(data, MP_INTERVAL_FIELD_ADJUST, <int64_t> interval.adjust.value)
    return data

cdef MPInterval interval_decode(const char ** p,
                                   uint32_t length) except*:
    cdef:
        char *end
        MPInterval interval
        uint8_t fields_count
        int64_t value
        uint8_t field_type
        mp_type field_value_type

    end = p[0] + length
    fields_count = mp_load_u8(p)
    length -= sizeof(uint8_t)
    if fields_count > 0 and length < 2:
        raise ValueError("Invalid MPInterval length")

    interval = <MPInterval> MPInterval.__new__(MPInterval)

    # NONE is default but it will be encoded,
    # and because zeros are not encoded then we must set a zero value
    interval.adjust = Adjust.EXCESS

    for i in range(fields_count):
        field_type = mp_load_u8(p)
        value = 0
        field_value_type = mp_typeof(p[0][0])
        if field_value_type == MP_UINT:
            if mp_check_uint(p[0], end) > 0:
                raise ValueError(f"invalid uint. field_type: {field_type}")

        elif field_value_type == MP_INT:
            if mp_check_int(p[0], end) > 0:
                raise ValueError(f"invalid int. field_type: {field_type}")

        else:
            raise ValueError("Invalid MPInterval field value type")

        if mp_read_int64(p, &value) != 0:
            raise ValueError("Invalid MPInterval value")

        if field_type == MP_INTERVAL_FIELD_YEAR:
            interval.year = value
        elif field_type == MP_INTERVAL_FIELD_MONTH:
            interval.month = value
        elif field_type == MP_INTERVAL_FIELD_WEEK:
            interval.week = value
        elif field_type == MP_INTERVAL_FIELD_DAY:
            interval.day = value
        elif field_type == MP_INTERVAL_FIELD_HOUR:
            interval.hour = value
        elif field_type == MP_INTERVAL_FIELD_MINUTE:
            interval.min = value
        elif field_type == MP_INTERVAL_FIELD_SECOND:
            interval.sec = value
        elif field_type == MP_INTERVAL_FIELD_NANOSECOND:
            interval.nsec = value
        elif field_type == MP_INTERVAL_FIELD_ADJUST:
            interval.adjust = Adjust(<int> value)
        else:
            raise ValueError(f"Invalid MPInterval field type {field_type}")

    return interval
