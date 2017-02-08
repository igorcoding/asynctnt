cimport cython
cimport cpython.bytes

from libc.stdint cimport uint32_t, uint64_t, int64_t

import hashlib


cdef class Request:
    @staticmethod
    cdef inline Request new(tnt.tp_request_type op,
                            uint64_t sync,
                            WriteBuffer buf):
        cdef Request req
        req = Request.__new__(Request)
        req.op = op
        req.sync = sync
        req.buf = buf
        req.waiter = None
        req.timeout_handle = None
        return req


cdef Request request_ping(bytes encoding, uint64_t sync):
    cdef:
        tnt.tp_request_type op
        WriteBuffer buf

    op = tnt.TP_PING
    buf = WriteBuffer.new(encoding)
    buf.write_header(sync, op)
    buf.write_length()
    return Request.new(op, sync, buf)


cdef Request request_call(bytes encoding, uint64_t sync,
                          str func_name, list args):
    cdef:
        tnt.tp_request_type op
        WriteBuffer buf

    op = tnt.TP_CALL
    buf = WriteBuffer.new(encoding)
    buf.write_header(sync, op)
    buf.encode_request_call(func_name, args)
    buf.write_length()
    return Request.new(op, sync, buf)


cdef Request request_call16(bytes encoding, uint64_t sync,
                            str func_name, list args):
    cdef:
        tnt.tp_request_type op
        WriteBuffer buf

    op = tnt.TP_CALL_16
    buf = WriteBuffer.new(encoding)
    buf.write_header(sync, op)
    buf.encode_request_call(func_name, args)
    buf.write_length()
    return Request.new(op, sync, buf)


cdef Request request_eval(bytes encoding, uint64_t sync,
                          str expression, list args):
    cdef:
        tnt.tp_request_type op
        WriteBuffer buf

    op = tnt.TP_EVAL
    buf = WriteBuffer.new(encoding)
    buf.write_header(sync, op)
    buf.encode_request_eval(expression, args)
    buf.write_length()
    return Request.new(op, sync, buf)


cdef Request request_select(bytes encoding, uint64_t sync,
                            uint32_t space, uint32_t index, list key,
                            uint64_t offset, uint64_t limit, uint32_t iterator):
    cdef:
        tnt.tp_request_type op
        WriteBuffer buf

    op = tnt.TP_SELECT
    buf = WriteBuffer.new(encoding)
    buf.write_header(sync, op)
    buf.encode_request_select(space, index, key,
                              offset, limit, iterator)
    buf.write_length()
    return Request.new(op, sync, buf)



cdef Request request_insert(bytes encoding, uint64_t sync,
                            uint32_t space, list t, bint replace):
    cdef:
        tnt.tp_request_type op
        WriteBuffer buf

    op = tnt.TP_INSERT if not replace else tnt.TP_REPLACE
    buf = WriteBuffer.new(encoding)
    buf.write_header(sync, op)
    buf.encode_request_insert(space, t)
    buf.write_length()
    return Request.new(op, sync, buf)


cdef Request request_delete(bytes encoding, uint64_t sync,
                            uint32_t space, uint32_t index, list key):
    cdef:
        tnt.tp_request_type op
        WriteBuffer buf

    op = tnt.TP_DELETE
    buf = WriteBuffer.new(encoding)
    buf.write_header(sync, op)
    buf.encode_request_delete(space, index, key)
    buf.write_length()
    return Request.new(op, sync, buf)


cdef Request request_update(bytes encoding, uint64_t sync,
                            uint32_t space, uint32_t index,
                            list key, list operations):
    cdef:
        tnt.tp_request_type op
        WriteBuffer buf

    op = tnt.TP_UPDATE
    buf = WriteBuffer.new(encoding)
    buf.write_header(sync, op)
    buf.encode_request_update(space, index, key, operations)
    buf.write_length()
    return Request.new(op, sync, buf)


cdef Request request_upsert(bytes encoding, uint64_t sync,
                            uint32_t space,
                            list t, list operations):
    cdef:
        tnt.tp_request_type op
        WriteBuffer buf

    op = tnt.TP_UPSERT
    buf = WriteBuffer.new(encoding)
    buf.write_header(sync, op)
    buf.encode_request_upsert(space, t, operations)
    buf.write_length()
    return Request.new(op, sync, buf)


cdef Request request_auth(bytes encoding, uint64_t sync,
                          bytes salt, str username, str password):
    cdef:
        tnt.tp_request_type op
        WriteBuffer buf

        bytes username_bytes, password_bytes
        bytes hash1, hash2, scramble

    op = tnt.TP_AUTH
    buf = WriteBuffer.new(encoding)
    buf.write_header(sync, op)

    username_bytes = encode_unicode_string(username, encoding)
    password_bytes = encode_unicode_string(password, encoding)

    hash1 = _sha1((password_bytes,))
    hash2 = _sha1((hash1,))
    scramble = _sha1((salt, hash2))
    scramble = _strxor(hash1, scramble)

    buf.encode_request_auth(username_bytes, scramble)
    buf.write_length()
    return Request.new(op, sync, buf)


cdef bytes _sha1(tuple values):
        cdef object sha = hashlib.sha1()
        for i in values:
            if i is not None:
                sha.update(i)
        return sha.digest()


cdef bytes _strxor(bytes hash1, bytes scramble):
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
