cimport cython


@cython.final
cdef class PushIterator:
    cdef:
        object _fut
        BaseRequest _request
        Response _response
