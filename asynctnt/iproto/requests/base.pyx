cimport cython
from libc.stdint cimport uint64_t, int64_t


@cython.freelist(REQUEST_FREELIST)
cdef class BaseRequest:

    # def __cinit__(self):
    #     self.sync = 0
    #     self.schema_id = -1
    #     self.parse_as_tuples = False
    #     self.parse_metadata = True
    #     self.push_subscribe = False

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
                               TntFields fields,
                               bint default_none) except NULL:
    if isinstance(t, list) or t is None:
        return buffer.mp_encode_list(p, <list> t)
    elif isinstance(t, tuple):
        return buffer.mp_encode_tuple(p, <tuple> t)
    elif isinstance(t, dict) and fields is not None:
        return buffer.mp_encode_list(
            p, dict_to_list_fields(<dict> t, fields, default_none)
        )
    else:
        if fields is not None:
            msg = 'sequence must be either list, tuple or dict'
        else:
            msg = 'sequence must be either list or tuple'
        raise TypeError(
            '{}, got: {}'.format(msg, type(t))
        )