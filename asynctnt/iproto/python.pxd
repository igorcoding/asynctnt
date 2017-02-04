cdef extern from "Python.h":
    char *PyByteArray_AS_STRING(object obj)
