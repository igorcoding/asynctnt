cimport cython

@cython.final
cdef class BeginRequest(BaseRequest):
    pass

@cython.final
cdef class CommitRequest(BaseRequest):
    pass

@cython.final
cdef class RollbackRequest(BaseRequest):
    pass
