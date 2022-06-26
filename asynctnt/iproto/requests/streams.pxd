cdef class BeginRequest(BaseRequest):
    cdef:
        uint32_t isolation
        double tx_timeout

cdef class CommitRequest(BaseRequest):
    pass

cdef class RollbackRequest(BaseRequest):
    pass
