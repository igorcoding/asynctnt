cdef class AuthRequest(BaseRequest):
    cdef:
        bytes salt
        str username
        str password
