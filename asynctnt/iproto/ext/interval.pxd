from libc.stdint cimport uint32_t


cdef class MPInterval:
    cdef:
        public int year
        public int month
        public int week
        public int day
        public int hour
        public int min
        public int sec
        public int nsec
        public object adjust

cdef enum mp_interval_fields:
    MP_INTERVAL_FIELD_YEAR = 0
    MP_INTERVAL_FIELD_MONTH = 1
    MP_INTERVAL_FIELD_WEEK = 2
    MP_INTERVAL_FIELD_DAY = 3
    MP_INTERVAL_FIELD_HOUR = 4
    MP_INTERVAL_FIELD_MINUTE = 5
    MP_INTERVAL_FIELD_SECOND = 6
    MP_INTERVAL_FIELD_NANOSECOND = 7
    MP_INTERVAL_FIELD_ADJUST = 8

cdef uint32_t interval_len(MPInterval interval)
cdef char *interval_encode(char *p, MPInterval interval) except NULL
cdef MPInterval interval_decode(const char ** p, uint32_t length) except *
