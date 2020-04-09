cimport cython
cimport cpython
cimport cpython.bytes

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
    cdef WriteBuffer encode(self, bytes encoding):
        cdef WriteBuffer buffer = WriteBuffer.create(encoding)
        buffer.write_header(self.sync, self.op, self.schema_id)
        username_bytes = encode_unicode_string(self.username, encoding)
        password_bytes = encode_unicode_string(self.password, encoding)

        hash1 = _sha1((password_bytes,))
        hash2 = _sha1((hash1,))
        scramble = _sha1((self.salt, hash2))
        scramble = _strxor(hash1, scramble)

        buffer.encode_request_auth(username_bytes, scramble)

        buffer.write_length()
        return buffer
