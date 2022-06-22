cdef class IDRequest(BaseRequest):
    cdef inline WriteBuffer encode(self, bytes encoding)
    cdef int encode_request(self, WriteBuffer buffer) except -1
