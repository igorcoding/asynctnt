cimport cpython
cimport cpython.bytes
cimport cython

import hashlib


cdef inline bytes _sha1(tuple values):
    cdef object sha = hashlib.sha1()
    for i in values:
        if i is not None:
            sha.update(i)
    return sha.digest()

cdef inline bytes _strxor(bytes hash1, bytes scramble):
    cdef:
        char *hash1_str
        ssize_t hash1_len

        char *scramble_str
        ssize_t scramble_len
    cpython.bytes.PyBytes_AsStringAndSize(hash1,
                                          &hash1_str, &hash1_len)
    cpython.bytes.PyBytes_AsStringAndSize(scramble,
                                          &scramble_str, &scramble_len)
    for i in range(scramble_len):
        scramble_str[i] = hash1_str[i] ^ scramble_str[i]
    return scramble

@cython.final
cdef class AuthRequest(BaseRequest):
    cdef int encode_body(self, WriteBuffer buffer) except -1:
        cdef:
            char *begin
            char *p
            uint32_t body_map_sz
            uint32_t max_body_len

            char *username_str
            ssize_t username_len

            char *scramble_str
            ssize_t scramble_len

        username_bytes = encode_unicode_string(self.username, buffer._encoding)
        password_bytes = encode_unicode_string(self.password, buffer._encoding)

        hash1 = _sha1((password_bytes,))
        hash2 = _sha1((hash1,))
        scramble = _sha1((self.salt, hash2))
        scramble = _strxor(hash1, scramble)

        cpython.bytes.PyBytes_AsStringAndSize(username_bytes,
                                              &username_str, &username_len)
        cpython.bytes.PyBytes_AsStringAndSize(scramble,
                                              &scramble_str, &scramble_len)
        body_map_sz = 2
        # Size description:
        # mp_sizeof_map()
        # + mp_sizeof_uint(TP_USERNAME)
        # + mp_sizeof_str(username_len)
        # + mp_sizeof_uint(TP_TUPLE)
        # + mp_sizeof_array(2)
        # + mp_sizeof_str(9) (chap-sha1)
        # + mp_sizeof_str(SCRAMBLE_SIZE)
        max_body_len = 1 \
                       + 1 \
                       + mp_sizeof_str(<uint32_t> username_len) \
                       + 1 \
                       + 1 \
                       + 1 + 9 \
                       + mp_sizeof_str(<uint32_t> scramble_len)

        buffer.ensure_allocated(max_body_len)

        p = begin = &buffer._buf[buffer._length]
        p = mp_encode_map(p, body_map_sz)
        p = mp_encode_uint(p, tarantool.IPROTO_USER_NAME)
        p = mp_encode_str(p, username_str, <uint32_t> username_len)

        p = mp_encode_uint(p, tarantool.IPROTO_TUPLE)
        p = mp_encode_array(p, 2)
        p = mp_encode_str(p, "chap-sha1", 9)
        p = mp_encode_str(p, scramble_str, <uint32_t> scramble_len)
        buffer._length += (p - begin)
