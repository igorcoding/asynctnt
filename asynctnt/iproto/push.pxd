
cdef class PushIterator:
    cdef:
        object _fut
        Request _request
        Response _response
