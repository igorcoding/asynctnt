cimport cpython
cimport cpython.bytes
cimport cpython.dict
cimport cpython.list
cimport cpython.tuple
cimport cpython.unicode
cimport cython
from cpython.datetime cimport datetime
from cpython.mem cimport PyMem_Free, PyMem_Malloc, PyMem_Realloc
from cpython.ref cimport PyObject
from libc.stdint cimport int64_t, uint8_t, uint32_t, uint64_t
from libc.stdio cimport printf
from libc.string cimport memcpy

from decimal import Decimal  # pragma: nocover
from uuid import UUID  # pragma: nocover


# noinspection PyUnresolvedReferences
# noinspection PyAttributeOutsideInit
@cython.no_gc_clear
@cython.final
@cython.freelist(_BUFFER_FREELIST_SIZE)
cdef class WriteBuffer:
    def __cinit__(self):
        self._smallbuf_inuse = True
        self._buf = self._smallbuf
        self._size = _BUFFER_INITIAL_SIZE
        self._length = 0
        self._encoding = None

    def __dealloc__(self):
        if self._buf is not NULL and not self._smallbuf_inuse:
            PyMem_Free(self._buf)
            self._buf = NULL
            self._size = 0

        if self._view_count:  # pragma: nocover
            raise RuntimeError(
                'Deallocating buffer with attached memoryviews')

    def __getbuffer__(self, Py_buffer *buffer, int flags):
        self._view_count += 1

        cpython.PyBuffer_FillInfo(
            buffer, self, self._buf, self._length,
            1,  # read-only
            flags
        )

    def __releasebuffer__(self, Py_buffer *buffer):
        self._view_count -= 1

    def hex(self):  # pragma: nocover
        return ":".join("{:02x}".format(ord(<bytes> (self._buf[i])))
                        for i in range(self._length))

    @staticmethod
    cdef WriteBuffer create(bytes encoding):
        cdef WriteBuffer buf
        buf = WriteBuffer.__new__(WriteBuffer)
        buf._encoding = encoding
        return buf

    cdef inline _check_readonly(self):  # pragma: nocover
        if self._view_count:
            raise BufferError('the buffer is in read-only mode')

    cdef inline len(self):
        return self._length

    cdef int ensure_allocated(self, ssize_t extra_length) except -1:
        cdef ssize_t new_size = extra_length + self._length

        if new_size > self._size:
            self._reallocate(new_size)

    cdef char *_ensure_allocated(self, char *p,
                                 ssize_t extra_length) except NULL:
        cdef:
            ssize_t new_size

        new_size = extra_length + self._length

        if new_size > self._size:
            used = p - self._buf
            self._reallocate(new_size)
            p = &self._buf[used]
        return p

    cdef int _reallocate(self, ssize_t new_size) except -1:
        cdef char *new_buf

        if new_size < _BUFFER_MAX_GROW:
            new_size = _BUFFER_MAX_GROW
        else:
            # Add a little extra
            new_size += _BUFFER_INITIAL_SIZE

        if self._smallbuf_inuse:
            new_buf = <char*> PyMem_Malloc(sizeof(char) * <size_t> new_size)
            if new_buf is NULL:  # pragma: nocover
                self._buf = NULL
                self._size = 0
                self._length = 0
                raise MemoryError
            memcpy(new_buf, self._buf, <size_t> self._size)
            self._size = new_size
            self._buf = new_buf
            self._smallbuf_inuse = False
        else:
            new_buf = <char*> PyMem_Realloc(<void*> self._buf, <size_t> new_size)
            if new_buf is NULL:
                PyMem_Free(self._buf)
                self._buf = NULL
                self._size = 0
                self._length = 0
                raise MemoryError
            self._buf = new_buf
            self._size = new_size

    cdef int write_buffer(self, WriteBuffer buf) except -1:
        if not buf._length:
            return 0

        self.ensure_allocated(buf._length)
        memcpy(self._buf + self._length,
               <void*> buf._buf,
               <size_t> buf._length)
        self._length += buf._length

    cdef int write_header(self, uint64_t sync,
                          tarantool.iproto_type op,
                          int64_t schema_id,
                          uint64_t stream_id) except -1:
        cdef:
            char *begin = NULL
            char *p = NULL
            uint32_t map_size
        self.ensure_allocated(HEADER_CONST_LEN)

        map_size = 2 \
                    + (<uint32_t> (schema_id > 0)) \
                    + (<uint32_t> (stream_id > 0))

        p = begin = &self._buf[self._length]
        p = mp_encode_map(&p[5], map_size)
        p = mp_encode_uint(p, tarantool.IPROTO_REQUEST_TYPE)
        p = mp_encode_uint(p, <uint32_t> op)
        p = mp_encode_uint(p, tarantool.IPROTO_SYNC)
        p = mp_encode_uint(p, sync)

        if schema_id > 0:  # pragma: nocover  # asynctnt does not send schema_id
            p = mp_encode_uint(p, tarantool.IPROTO_SCHEMA_VERSION)
            p = mp_store_u8(p, 0xce)
            p = mp_store_u32(p, schema_id)

        if stream_id > 0:
            p = mp_encode_uint(p, tarantool.IPROTO_STREAM_ID)
            p = mp_encode_uint(p, stream_id)

        self._length += (p - begin)

    cdef void write_length(self):
        cdef:
            char *p
        p = self._buf
        p = mp_store_u8(p, 0xce)
        p = mp_store_u32(p, self._length - 5)

    cdef char *mp_encode_nil(self, char *p) except NULL:
        cdef char *begin
        p = begin = self._ensure_allocated(p, 1)
        p = mp_encode_nil(p)
        self._length += (p - begin)
        return p

    cdef char *mp_encode_bool(self, char *p, bint value) except NULL:
        cdef char *begin
        p = begin = self._ensure_allocated(p, 1)
        p = mp_encode_bool(p, value)
        self._length += (p - begin)
        return p

    cdef char *mp_encode_double(self, char *p, double value) except NULL:
        cdef char *begin
        p = begin = self._ensure_allocated(p, mp_sizeof_double(value))
        p = mp_encode_double(p, value)
        self._length += (p - begin)
        return p

    cdef char *mp_encode_uint(self, char *p, uint64_t value) except NULL:
        cdef char *begin
        p = begin = self._ensure_allocated(p, mp_sizeof_uint(value))
        p = mp_encode_uint(p, value)
        self._length += (p - begin)
        return p

    cdef char *mp_encode_int(self, char *p, int64_t value) except NULL:
        cdef char *begin
        p = begin = self._ensure_allocated(p, mp_sizeof_int(value))
        p = mp_encode_int(p, value)
        self._length += (p - begin)
        return p

    cdef char *mp_encode_str(self, char *p,
                             const char *str, uint32_t len) except NULL:
        cdef char *begin
        p = begin = self._ensure_allocated(p, mp_sizeof_str(len))
        p = mp_encode_str(p, str, len)
        self._length += (p - begin)
        return p

    cdef char *mp_encode_bin(self, char *p,
                             const char *data, uint32_t len) except NULL:
        cdef char *begin
        p = begin = self._ensure_allocated(p, mp_sizeof_bin(len))
        p = mp_encode_bin(p, data, len)
        self._length += (p - begin)
        return p

    cdef char *mp_encode_decimal(self, char *p, object value) except NULL:
        cdef:
            char *begin
            uint8_t sign
            tuple digits
            uint32_t digits_count
            int exponent
            uint32_t length

        decimal_tuple = value.as_tuple()
        sign = <uint8_t> decimal_tuple.sign
        digits = <tuple> decimal_tuple.digits
        exponent = <int> decimal_tuple.exponent

        digits_count = <uint32_t> len(digits)
        length = decimal_len(exponent, digits_count)

        p = begin = self._ensure_allocated(p, mp_sizeof_ext(length))

        # encode header
        p = mp_encode_extl(p, tarantool.MP_DECIMAL, length)

        # encode decimal
        p = decimal_encode(p, digits_count, sign, digits, exponent)

        self._length += (p - begin)
        return p

    cdef char *mp_encode_uuid(self, char *p, object value) except NULL:
        cdef:
            char *begin
            char *data_p

        p = begin = self._ensure_allocated(p, mp_sizeof_ext(16))
        data_p = cpython.bytes.PyBytes_AS_STRING(<bytes> value.bytes)

        # encode header
        p = mp_encode_ext(p, tarantool.MP_UUID, data_p, 16)  # uuid is exactly 16 bytes

        self._length += (p - begin)
        return p

    cdef char *mp_encode_datetime(self, char *p, object value) except NULL:
        cdef:
            char *begin
            uint32_t length
            datetime pydt
            IProtoDateTime dt

        pydt = <datetime> value
        datetime_zero(&dt)
        datetime_from_py(pydt, &dt)

        length = datetime_len(&dt)
        p = begin = self._ensure_allocated(p, mp_sizeof_ext(length))
        p = mp_encode_extl(p, tarantool.MP_DATETIME, length)
        p = datetime_encode(p, &dt)
        self._length += (p - begin)
        return p

    cdef char *mp_encode_array(self, char *p, uint32_t len) except NULL:
        cdef char *begin
        p = begin = self._ensure_allocated(p, mp_sizeof_array(len))
        p = mp_encode_array(p, len)
        self._length += (p - begin)
        return p

    cdef char *mp_encode_map(self, char *p, uint32_t len) except NULL:
        cdef char *begin
        p = begin = self._ensure_allocated(p, mp_sizeof_map(len))
        p = mp_encode_map(p, len)
        self._length += (p - begin)
        return p

    cdef char *mp_encode_list(self, char *p, list arr) except NULL:
        cdef:
            uint32_t arr_len
            PyObject *item_ptr
            object item

        if arr is not None:
            arr_len = <uint32_t> cpython.list.PyList_GET_SIZE(arr)
        else:  # pragma: nocover
            arr_len = 0
        p = self.mp_encode_array(p, arr_len)

        if arr_len > 0:
            for i in range(arr_len):
                item_ptr = cpython.list.PyList_GET_ITEM(arr, i)
                item = <object> item_ptr
                p = self.mp_encode_obj(p, item)
        return p

    cdef char *mp_encode_tuple(self, char *p, tuple t) except NULL:
        cdef:
            uint32_t t_len
            PyObject *item_ptr
            object item

        if t is not None:
            t_len = <uint32_t> cpython.tuple.PyTuple_GET_SIZE(t)
        else:  # pragma: nocover
            t_len = 0
        p = self.mp_encode_array(p, t_len)

        if t_len > 0:
            for i in range(t_len):
                item_ptr = cpython.tuple.PyTuple_GET_ITEM(t, i)
                item = <object> item_ptr
                p = self.mp_encode_obj(p, item)
        return p

    cdef char *mp_encode_dict(self, char *p, dict d) except NULL:
        cdef:
            uint32_t d_len
            PyObject *pkey
            PyObject *pvalue
            object key, value
            Py_ssize_t pos

        if d is not None:
            d_len = <uint32_t> cpython.dict.PyDict_Size(d)
        else:  # pragma: nocover
            d_len = 0
        p = self.mp_encode_map(p, d_len)

        pos = 0
        while cpython.dict.PyDict_Next(d, &pos, &pkey, &pvalue):
            key = <object> pkey
            value = <object> pvalue
            p = self.mp_encode_obj(p, key)
            p = self.mp_encode_obj(p, value)

        return p

    cdef char *mp_encode_obj(self, char *p, object o) except NULL:
        cdef:
            bytes o_string_temp
            char *o_string_str
            ssize_t o_string_len

        if o is None:
            return self.mp_encode_nil(p)

        elif isinstance(o, float):
            return self.mp_encode_double(p, <double> o)

        elif isinstance(o, bool):
            return self.mp_encode_bool(p, <bint> o)

        elif isinstance(o, int):
            if o >= 0:
                return self.mp_encode_uint(p, <uint64_t> o)
            else:
                return self.mp_encode_int(p, <int64_t> o)

        elif isinstance(o, bytes):
            o_string_str = NULL
            o_string_len = 0
            cpython.bytes.PyBytes_AsStringAndSize(o,
                                                  &o_string_str,
                                                  &o_string_len)
            return self.mp_encode_bin(p, o_string_str, <uint32_t> o_string_len)

        elif isinstance(o, str):
            o_string_temp = encode_unicode_string(o, self._encoding)
            o_string_str = NULL
            o_string_len = 0
            cpython.bytes.PyBytes_AsStringAndSize(o_string_temp,
                                                  &o_string_str,
                                                  &o_string_len)

            p = self.mp_encode_str(p, o_string_str, <uint32_t> o_string_len)
            return p

        elif isinstance(o, list):
            return self.mp_encode_list(p, <list> o)

        elif isinstance(o, tuple):
            return self.mp_encode_tuple(p, <tuple> o)

        elif isinstance(o, dict):
            return self.mp_encode_dict(p, <dict> o)

        elif isinstance(o, datetime):
            return self.mp_encode_datetime(p, o)

        elif isinstance(o, Decimal):
            return self.mp_encode_decimal(p, o)

        elif isinstance(o, UUID):
            return self.mp_encode_uuid(p, o)

        else:
            raise TypeError(
                'Type `{}` is not supported for encoding'.format(type(o)))
