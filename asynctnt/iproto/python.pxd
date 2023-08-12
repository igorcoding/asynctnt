from cpython.version cimport PY_VERSION_HEX


cdef extern from "Python.h":
    char *PyByteArray_AS_STRING(object obj)
    int Py_REFCNT(object obj)


cdef extern from "datetime.h":
    """
    /* Backport for Python 2.x */
    #if PY_MAJOR_VERSION < 3
        #ifndef PyDateTime_DELTA_GET_DAYS
            #define PyDateTime_DELTA_GET_DAYS(o) (((PyDateTime_Delta*)o)->days)
        #endif
        #ifndef PyDateTime_DELTA_GET_SECONDS
            #define PyDateTime_DELTA_GET_SECONDS(o) (((PyDateTime_Delta*)o)->seconds)
        #endif
        #ifndef PyDateTime_DELTA_GET_MICROSECONDS
            #define PyDateTime_DELTA_GET_MICROSECONDS(o) (((PyDateTime_Delta*)o)->microseconds)
        #endif
    #endif
    /* Backport for Python < 3.6 */
    #if PY_VERSION_HEX < 0x030600a4
        #ifndef PyDateTime_TIME_GET_FOLD
            #define PyDateTime_TIME_GET_FOLD(o) ((void)(o), 0)
        #endif
        #ifndef PyDateTime_DATE_GET_FOLD
            #define PyDateTime_DATE_GET_FOLD(o) ((void)(o), 0)
        #endif
    #endif
    /* Backport for Python < 3.6 */
    #if PY_VERSION_HEX < 0x030600a4
        #define __Pyx_DateTime_DateTimeWithFold(year, month, day, hour, minute, second, microsecond, tz, fold) \
            ((void)(fold), PyDateTimeAPI->DateTime_FromDateAndTime(year, month, day, hour, minute, second, \
                microsecond, tz, PyDateTimeAPI->DateTimeType))
        #define __Pyx_DateTime_TimeWithFold(hour, minute, second, microsecond, tz, fold) \
            ((void)(fold), PyDateTimeAPI->Time_FromTime(hour, minute, second, microsecond, tz, PyDateTimeAPI->TimeType))
    #else /* For Python 3.6+ so that we can pass tz */
        #define __Pyx_DateTime_DateTimeWithFold(year, month, day, hour, minute, second, microsecond, tz, fold) \
            PyDateTimeAPI->DateTime_FromDateAndTimeAndFold(year, month, day, hour, minute, second, \
                microsecond, tz, fold, PyDateTimeAPI->DateTimeType)
        #define __Pyx_DateTime_TimeWithFold(hour, minute, second, microsecond, tz, fold) \
            PyDateTimeAPI->Time_FromTimeAndFold(hour, minute, second, microsecond, tz, fold, PyDateTimeAPI->TimeType)
    #endif
    /* Backport for Python < 3.7 */
    #if PY_VERSION_HEX < 0x030700b1
        #define __Pyx_TimeZone_UTC NULL
        #define __Pyx_TimeZone_FromOffset(offset) ((void)(offset), (PyObject*)NULL)
        #define __Pyx_TimeZone_FromOffsetAndName(offset, name) ((void)(offset), (void)(name), (PyObject*)NULL)
    #else
        #define __Pyx_TimeZone_UTC PyDateTime_TimeZone_UTC
        #define __Pyx_TimeZone_FromOffset(offset) PyTimeZone_FromOffset(offset)
        #define __Pyx_TimeZone_FromOffsetAndName(offset, name) PyTimeZone_FromOffsetAndName(offset, name)
    #endif
    /* Backport for Python < 3.10 */
    #if PY_VERSION_HEX < 0x030a00a1
        #ifndef PyDateTime_TIME_GET_TZINFO
            #define PyDateTime_TIME_GET_TZINFO(o) \
                ((((PyDateTime_Time*)o)->hastzinfo) ? ((PyDateTime_Time*)o)->tzinfo : Py_None)
        #endif
        #ifndef PyDateTime_DATE_GET_TZINFO
            #define PyDateTime_DATE_GET_TZINFO(o) \
                ((((PyDateTime_DateTime*)o)->hastzinfo) ? ((PyDateTime_DateTime*)o)->tzinfo : Py_None)
        #endif
    #endif
    """

    # The above macros is Python 3.7+ so we use these instead
    object __Pyx_TimeZone_FromOffset(object offset)


cdef inline object timezone_new(object offset):
    if PY_VERSION_HEX < 0x030700b1:
        from datetime import timezone
        return timezone(offset)
    return __Pyx_TimeZone_FromOffset(offset)
