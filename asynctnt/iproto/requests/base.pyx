cimport cython
from libc.stdint cimport int64_t, uint64_t


@cython.freelist(REQUEST_FREELIST)
cdef class BaseRequest:

    # def __cinit__(self):
    #     self.sync = 0
    #     self.schema_id = -1
    #     self.parse_as_tuples = False
    #     self.parse_metadata = True
    #     self.push_subscribe = False

    cdef inline WriteBuffer encode(self, bytes encoding):
        cdef WriteBuffer buffer = WriteBuffer.create(encoding)
        buffer.write_header(self.sync, self.op, self.schema_id, self.stream_id)
        self.encode_body(buffer)
        buffer.write_length()
        return buffer

    cdef int encode_body(self, WriteBuffer buffer) except -1:
        return 0

    def __repr__(self):  # pragma: nocover
        return \
            '<Request op={} sync={} schema_id={} push_subscribe={}>'.format(
                self.op,
                self.sync,
                self.schema_id,
                self.push_subscribe
            )


cdef char *encode_key_sequence(WriteBuffer buffer,
                               char *p, object t,
                               Metadata metadata,
                               bint default_none) except NULL:
    if isinstance(t, list) or t is None:
        return buffer.mp_encode_list(p, <list> t)
    elif isinstance(t, tuple):
        return buffer.mp_encode_tuple(p, <tuple> t)
    elif isinstance(t, dict) and metadata is not None:
        return buffer.mp_encode_list(
            p, dict_to_list_fields(<dict> t, metadata, default_none)
        )
    else:
        if metadata is not None:
            msg = 'sequence must be either list, tuple or dict'
        else:
            msg = 'sequence must be either list or tuple'
        raise TypeError(
            '{}, got: {}'.format(msg, type(t))
        )
