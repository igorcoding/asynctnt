cimport cpython.unicode
from cpython.ref cimport PyObject


cdef bytes encode_unicode_string(object s, bytes encoding=b'utf-8'):
    cdef:
        bytes b
        PyObject *p

    b = <bytes><object>cpython.unicode.PyUnicode_AsEncodedString(
        s, encoding, b'strict'
    )
    return b


cdef str decode_string(bytes b, bytes encoding=b'utf-8'):
    return <str><object>cpython.unicode.PyUnicode_FromEncodedObject(
        b, encoding, b'strict'
    )
