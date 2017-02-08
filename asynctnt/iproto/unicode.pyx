cimport cpython.unicode
from cpython.ref cimport PyObject


cdef bytes encode_unicode_string(str s, bytes encoding=b'utf-8'):
    cdef:
        bytes b
        PyObject *p

    b = <bytes><object>cpython.unicode.PyUnicode_AsEncodedString(
        s, encoding, b'strict'
    )
    return b
