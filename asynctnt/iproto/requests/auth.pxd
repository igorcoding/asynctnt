cdef class AuthRequest(BaseRequest):
    cdef:
        bytes salt
        str username
        str password

    cdef inline WriteBuffer encode(self, bytes encoding)
