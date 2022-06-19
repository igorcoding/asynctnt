cdef class AuthRequest(BaseRequest):
    cdef:
        bytes salt
        str username
        str password

    cdef inline WriteBuffer encode(self, bytes encoding)
    cdef int encode_request_auth(self,
                                 WriteBuffer buffer,
                                 bytes username,
                                 bytes scramble) except -1