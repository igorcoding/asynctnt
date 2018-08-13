cimport cpython
cimport cython
cimport cpython.bytes
cimport cpython.list
cimport cpython.tuple
cimport cpython.dict
cimport cpython.unicode

from cpython.mem cimport PyMem_Malloc, PyMem_Realloc, PyMem_Free
from cpython.ref cimport PyObject

from libc.string cimport memcpy
from libc.stdint cimport uint32_t, uint64_t, int64_t


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
        self.__op_offset = -1
        self.__sync_offset = -1
        self.__schema_id_offset = -1

    def __dealloc__(self):
        if self._buf is not NULL and not self._smallbuf_inuse:
            PyMem_Free(self._buf)
            self._buf = NULL
            self._size = 0

        if self._view_count:
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

    def hex(self):
        return ":".join("{:02x}".format(ord(<bytes>(self._buf[i])))
                        for i in range(self._length))

    @staticmethod
    cdef WriteBuffer new(bytes encoding):
        cdef WriteBuffer buf
        buf = WriteBuffer.__new__(WriteBuffer)
        buf._encoding = encoding
        return buf

    cdef inline _check_readonly(self):
        if self._view_count:
            raise BufferError('the buffer is in read-only mode')

    cdef inline len(self):
        return self._length

    cdef void ensure_allocated(self, ssize_t extra_length) except *:
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

    cdef void _reallocate(self, ssize_t new_size) except *:
        cdef char *new_buf

        if new_size < _BUFFER_MAX_GROW:
            new_size = _BUFFER_MAX_GROW
        else:
            # Add a little extra
            new_size += _BUFFER_INITIAL_SIZE

        if self._smallbuf_inuse:
            new_buf = <char*>PyMem_Malloc(sizeof(char) * <size_t>new_size)
            if new_buf is NULL:
                self._buf = NULL
                self._size = 0
                self._length = 0
                raise MemoryError
            memcpy(new_buf, self._buf, <size_t>self._size)
            self._size = new_size
            self._buf = new_buf
            self._smallbuf_inuse = False
        else:
            new_buf = <char*>PyMem_Realloc(<void*>self._buf, <size_t>new_size)
            if new_buf is NULL:
                PyMem_Free(self._buf)
                self._buf = NULL
                self._size = 0
                self._length = 0
                raise MemoryError
            self._buf = new_buf
            self._size = new_size

    cdef void write_buffer(self, WriteBuffer buf) except *:
        if not buf._length:
            return

        self.ensure_allocated(buf._length)
        memcpy(self._buf + self._length,
               <void*>buf._buf,
               <size_t>buf._length)
        self._length += buf._length

    cdef void write_header(self, uint64_t sync,
                           tarantool.iproto_type op,
                           int64_t schema_id) except *:
        cdef:
            char *begin = NULL
            char *p = NULL
            uint32_t map_size
        self.ensure_allocated(HEADER_CONST_LEN)

        map_size = 2 + <uint32_t>(schema_id > 0)

        p = begin = &self._buf[self._length]
        p = mp_encode_map(&p[5], map_size)
        p = mp_encode_uint(p, tarantool.IPROTO_REQUEST_TYPE)
        self.__op_offset = (p - begin)  # save op position
        p = mp_encode_uint(p, <uint32_t>op)
        p = mp_encode_uint(p, tarantool.IPROTO_SYNC)
        self.__sync_offset = (p - begin)  # save sync position
        p = mp_encode_uint(p, sync)

        if schema_id > 0:
            p = mp_encode_uint(p, tarantool.IPROTO_SCHEMA_VERSION)
            p = mp_store_u8(p, 0xce)
            self.__schema_id_offset = (p - begin)  # save schema_id position
            p = mp_store_u32(p, schema_id)

        self._length += (p - begin)

    cdef void change_schema_id(self, int64_t new_schema_id):
        cdef char *p

        if self.__schema_id_offset > 0 and new_schema_id > 0:
            p = &self._buf[self.__schema_id_offset]
            p = mp_store_u32(p, new_schema_id)

    cdef void write_length(self):
        cdef:
            char *p
        p = self._buf
        p = mp_store_u8(p, 0xce)
        p = mp_store_u32(p, self._length - 5)

    cdef char *_encode_nil(self, char *p) except NULL:
        cdef char *begin
        p = begin = self._ensure_allocated(p, 1)
        p = mp_encode_nil(p)
        self._length += (p - begin)
        return p

    cdef char *_encode_bool(self, char *p, bint value) except NULL:
        cdef char *begin
        p = begin = self._ensure_allocated(p, 1)
        p = mp_encode_bool(p, value)
        self._length += (p - begin)
        return p

    cdef char *_encode_double(self, char *p, double value) except NULL:
        cdef char *begin
        p = begin = self._ensure_allocated(p, mp_sizeof_double(value))
        p = mp_encode_double(p, value)
        self._length += (p - begin)
        return p

    cdef char *_encode_uint(self, char *p, uint64_t value) except NULL:
        cdef char *begin
        p = begin = self._ensure_allocated(p, mp_sizeof_uint(value))
        p = mp_encode_uint(p, value)
        self._length += (p - begin)
        return p

    cdef char *_encode_int(self, char *p, int64_t value) except NULL:
        cdef char *begin
        p = begin = self._ensure_allocated(p, mp_sizeof_int(value))
        p = mp_encode_int(p, value)
        self._length += (p - begin)
        return p

    cdef char *_encode_str(self, char *p,
                           const char *str, uint32_t len) except NULL:
        cdef char *begin
        p = begin = self._ensure_allocated(p, mp_sizeof_str(len))
        p = mp_encode_str(p, str, len)
        self._length += (p - begin)
        return p

    cdef char *_encode_bin(self, char *p,
                           const char *data, uint32_t len) except NULL:
        cdef char *begin
        p = begin = self._ensure_allocated(p, mp_sizeof_bin(len))
        p = mp_encode_bin(p, data, len)
        self._length += (p - begin)
        return p

    cdef char *_encode_array(self, char *p, uint32_t len) except NULL:
        cdef char *begin
        p = begin = self._ensure_allocated(p, mp_sizeof_array(len))
        p = mp_encode_array(p, len)
        self._length += (p - begin)
        return p

    cdef char *_encode_map(self, char *p, uint32_t len) except NULL:
        cdef char *begin
        p = begin = self._ensure_allocated(p, mp_sizeof_map(len))
        p = mp_encode_map(p, len)
        self._length += (p - begin)
        return p

    cdef char *_encode_list(self, char *p, list arr) except NULL:
        cdef:
            uint32_t arr_len
            PyObject *item_ptr
            object item

        if arr is not None:
            arr_len = <uint32_t>cpython.list.PyList_GET_SIZE(arr)
        else:
            arr_len = 0
        p = self._encode_array(p, arr_len)

        if arr_len > 0:
            for i in range(arr_len):
                item_ptr = cpython.list.PyList_GET_ITEM(arr, i)
                item = <object>item_ptr
                p = self._encode_obj(p, item)
        return p

    cdef char *_encode_tuple(self, char *p, tuple t) except NULL:
        cdef:
            uint32_t t_len
            PyObject *item_ptr
            object item

        if t is not None:
            t_len = <uint32_t>cpython.tuple.PyTuple_GET_SIZE(t)
        else:
            t_len = 0
        p = self._encode_array(p, t_len)

        if t_len > 0:
            for i in range(t_len):
                item_ptr = cpython.tuple.PyTuple_GET_ITEM(t, i)
                item = <object>item_ptr
                p = self._encode_obj(p, item)
        return p

    cdef char *_encode_dict(self, char *p, dict d) except NULL:
        cdef:
            uint32_t d_len
            PyObject *pkey
            PyObject *pvalue
            object key, value
            Py_ssize_t pos

        if d is not None:
            d_len = <uint32_t>cpython.dict.PyDict_Size(d)
        else:
            d_len = 0
        p = self._encode_map(p, d_len)

        pos = 0
        while cpython.dict.PyDict_Next(d, &pos, &pkey, &pvalue):
            key = <object>pkey
            value = <object>pvalue
            p = self._encode_obj(p, key)
            p = self._encode_obj(p, value)

        return p

    cdef char *_encode_key_sequence(self, char *p, t,
                                    TntFields fields=None,
                                    bint default_none=False) except NULL:
        if isinstance(t, list) or t is None:
            return self._encode_list(p, <list>t)
        elif isinstance(t, tuple):
            return self._encode_tuple(p, <tuple>t)
        elif isinstance(t, dict) and fields is not None:
            return self._encode_list(
                p, dict_to_list_fields(<dict>t, fields, default_none)
            )
        else:
            if fields is not None:
                msg = 'sequence must be either list, tuple or dict'
            else:
                msg = 'sequence must be either list or tuple'
            raise TypeError(
                '{}, got: {}'.format(msg, type(t))
            )

    cdef char *_encode_obj(self, char *p, object o) except NULL:
        cdef:
            bytes o_string_temp
            char *o_string_str
            ssize_t o_string_len

        if o is None:
            return self._encode_nil(p)

        elif isinstance(o, bool):
            return self._encode_bool(p, <bint>o)

        elif isinstance(o, float):
            return self._encode_double(p, <double>o)

        elif isinstance(o, int):
            if o >= 0:
                return self._encode_uint(p, <uint64_t>o)
            else:
                return self._encode_int(p, <int64_t>o)

        elif isinstance(o, bytes):
            o_string_str = NULL
            o_string_len = 0
            cpython.bytes.PyBytes_AsStringAndSize(o,
                                                  &o_string_str,
                                                  &o_string_len)
            return self._encode_bin(p, o_string_str, <uint32_t>o_string_len)

        elif isinstance(o, str):
            o_string_temp = encode_unicode_string(o, self._encoding)
            o_string_str = NULL
            o_string_len = 0
            cpython.bytes.PyBytes_AsStringAndSize(o_string_temp,
                                                  &o_string_str,
                                                  &o_string_len)

            p = self._encode_str(p, o_string_str, <uint32_t>o_string_len)
            return p

        elif isinstance(o, list):
            return self._encode_list(p, <list>o)

        elif isinstance(o, tuple):
            return self._encode_tuple(p, <tuple>o)

        elif isinstance(o, dict):
            return self._encode_dict(p, <dict>o)

        else:
            raise TypeError(
                'Type `{}` is not supported for encoding'.format(type(o)))

    cdef char *_encode_update_ops(self, char *p, list operations,
                                  SchemaSpace space) except NULL:
        cdef:
            char *begin
            uint32_t ops_len, op_len
            bytes str_temp
            char *str_c
            ssize_t str_len

            char *op_str_c
            ssize_t op_str_len
            char op

            uint32_t extra_length

            uint64_t field_no
            object field_no_obj

            uint32_t splice_position, splice_offset

        begin = NULL

        if operations is not None:
            ops_len = <uint32_t>cpython.list.PyList_GET_SIZE(operations)
        else:
            ops_len = 0

        p = self._encode_array(p, ops_len)
        if ops_len == 0:
            return p

        for operation in operations:
            if not isinstance(operation, (list, tuple)):
                raise TypeError(
                    'Single operation must be a tuple or list')

            op_len = <uint32_t>cpython.list.PyList_GET_SIZE(operation)
            if op_len < 3:
                raise IndexError(
                    'Operation length must be at least 3')

            op_type_str = operation[0]
            if isinstance(op_type_str, str):
                str_temp = encode_unicode_string(op_type_str, self._encoding)
            elif isinstance(op_type_str, bytes):
                str_temp = <bytes>op_type_str
            else:
                raise TypeError(
                    'Operation type must of a str or bytes type')

            field_no_obj = operation[1]
            if isinstance(field_no_obj, int):
                field_no = <uint64_t>field_no_obj
            elif isinstance(field_no_obj, str):
                if space.fields is not None:
                    field_no = <uint64_t>space.fields.id_by_name(field_no_obj)
                else:
                    raise TypeError(
                        'Operation field_no must be int as there is '
                        'no format declaration in space {}'.format(space.sid))
            else:
                raise TypeError(
                    'Operation field_no must be of either int or str type')

            cpython.bytes.PyBytes_AsStringAndSize(str_temp, &op_str_c,
                                                  &op_str_len)
            op = <char>0
            if op_str_len == 1:
                op = op_str_c[0]

            if op == tarantool.IPROTO_OP_ADD \
                    or op == tarantool.IPROTO_OP_SUB \
                    or op == tarantool.IPROTO_OP_AND \
                    or op == tarantool.IPROTO_OP_XOR \
                    or op == tarantool.IPROTO_OP_OR \
                    or op == tarantool.IPROTO_OP_DELETE:
                op_argument = operation[2]
                if not isinstance(op_argument, int):
                    raise TypeError(
                        'int argument required for '
                        'Arithmetic and Delete operations'
                    )
                # mp_sizeof_array(3)
                # + mp_sizeof_str(1)
                # + mp_sizeof_uint(field_no)
                extra_length = 1 + 2 + mp_sizeof_uint(field_no)
                p = begin = self._ensure_allocated(p, extra_length)

                p = mp_encode_array(p, 3)
                p = mp_encode_str(p, op_str_c, 1)
                p = mp_encode_uint(p, field_no)
                self._length += (p - begin)
                p = self._encode_obj(p, op_argument)
            elif op == tarantool.IPROTO_OP_INSERT \
                    or op == tarantool.IPROTO_OP_ASSIGN:
                op_argument = operation[2]

                # mp_sizeof_array(3)
                # + mp_sizeof_str(1)
                # + mp_sizeof_uint(field_no)
                extra_length = 1 + 2 + mp_sizeof_uint(field_no)
                p = begin = self._ensure_allocated(p, extra_length)

                p = mp_encode_array(p, 3)
                p = mp_encode_str(p, op_str_c, 1)
                p = mp_encode_uint(p, field_no)
                self._length += (p - begin)
                p = self._encode_obj(p, op_argument)

            elif op == tarantool.IPROTO_OP_SPLICE:
                if op_len < 5:
                    raise IndexError(
                        'Splice operation must have length of 5, '
                        'but got: {}'.format(op_len)
                    )

                splice_position_obj = operation[2]
                splice_offset_obj = operation[3]
                op_argument = operation[4]
                if not isinstance(splice_position_obj, int):
                    raise TypeError('Splice position must be int')
                if not isinstance(splice_offset_obj, int):
                    raise TypeError('Splice offset must be int')

                splice_position = <uint32_t>splice_position_obj
                splice_offset = <uint32_t>splice_offset_obj

                # mp_sizeof_array(5) + mp_sizeof_str(1) + ...
                extra_length = 1 + 2 \
                                + mp_sizeof_uint(field_no) \
                                + mp_sizeof_uint(splice_position) \
                                + mp_sizeof_uint(splice_offset)
                p = begin = self._ensure_allocated(p, extra_length)

                p = mp_encode_array(p, 5)
                p = mp_encode_str(p, op_str_c, 1)
                p = mp_encode_uint(p, field_no)
                p = mp_encode_uint(p, splice_position)
                p = mp_encode_uint(p, splice_offset)
                self._length += (p - begin)
                p = self._encode_obj(p, op_argument)
            else:
                raise TypeError(
                    'Unknown update operation type `{}`'.format(op_type_str))
        return p

    cdef void encode_request_call(self, str func_name, args) except *:
        cdef:
            char *begin
            char *p
            uint32_t body_map_sz
            uint32_t max_body_len

            bytes func_name_temp
            char *func_name_str
            ssize_t func_name_len

        func_name_str = NULL
        func_name_len = 0

        func_name_temp = encode_unicode_string(func_name, self._encoding)
        cpython.bytes.PyBytes_AsStringAndSize(func_name_temp,
                                              &func_name_str,
                                              &func_name_len)
        body_map_sz = 2
        # Size description:
        # mp_sizeof_map()
        # + mp_sizeof_uint(TP_FUNCTION)
        # + mp_sizeof_str(func_name)
        # + mp_sizeof_uint(TP_TUPLE)
        max_body_len = 1 \
                       + 1 \
                       + mp_sizeof_str(<uint32_t>func_name_len) \
                       + 1

        self.ensure_allocated(max_body_len)

        p = begin = &self._buf[self._length]
        p = mp_encode_map(p, body_map_sz)
        p = mp_encode_uint(p, tarantool.IPROTO_FUNCTION_NAME)
        p = mp_encode_str(p, func_name_str, <uint32_t>func_name_len)

        p = mp_encode_uint(p, tarantool.IPROTO_TUPLE)
        self._length += (p - begin)
        p = self._encode_key_sequence(p, args)

    cdef void encode_request_eval(self, str expression, args) except *:
        cdef:
            char *begin
            char *p
            uint32_t body_map_sz
            uint32_t max_body_len

            bytes expression_temp
            char *expression_str
            ssize_t expression_len

        expression_str = NULL
        expression_len = 0

        expression_temp = encode_unicode_string(expression, self._encoding)
        cpython.bytes.PyBytes_AsStringAndSize(expression_temp,
                                              &expression_str,
                                              &expression_len)
        body_map_sz = 2
        # Size description:
        # mp_sizeof_map()
        # + mp_sizeof_uint(TP_EXPRESSION)
        # + mp_sizeof_str(expression)
        # + mp_sizeof_uint(TP_TUPLE)
        max_body_len = 1 \
                       + 1 \
                       + mp_sizeof_str(<uint32_t>expression_len) \
                       + 1

        self.ensure_allocated(max_body_len)

        p = begin = &self._buf[self._length]
        p = mp_encode_map(p, body_map_sz)
        p = mp_encode_uint(p, tarantool.IPROTO_EXPR)
        p = mp_encode_str(p, expression_str, <uint32_t>expression_len)

        p = mp_encode_uint(p, tarantool.IPROTO_TUPLE)
        self._length += (p - begin)
        p = self._encode_key_sequence(p, args)

    cdef void encode_request_select(self, SchemaSpace space, SchemaIndex index,
                                    key, uint64_t offset, uint64_t limit,
                                    uint32_t iterator) except *:
        cdef:
            char *begin
            char *p
            uint32_t body_map_sz
            uint32_t max_body_len
            uint32_t space_id, index_id

        space_id = space.sid
        index_id = index.iid

        body_map_sz = 3 \
                      + <uint32_t>(index_id > 0) \
                      + <uint32_t>(offset > 0) \
                      + <uint32_t>(iterator > 0)
        # Size description:
        # mp_sizeof_map(body_map_sz)
        # + mp_sizeof_uint(TP_SPACE)
        # + mp_sizeof_uint(space)
        # + mp_sizeof_uint(TP_LIMIT)
        # + mp_sizeof_uint(limit)
        max_body_len = 1 \
                       + 1 \
                       + 9 \
                       + 1 \
                       + 9

        if index_id > 0:
            # mp_sizeof_uint(TP_INDEX) + mp_sizeof_uint(index_id)
            max_body_len += 1 + 9
        if offset > 0:
            # mp_sizeof_uint(TP_OFFSET) + mp_sizeof_uint(offset)
            max_body_len += 1 + 9
        if iterator > 0:
            # mp_sizeof_uint(TP_ITERATOR) + mp_sizeof_uint(iterator)
            max_body_len += 1 + 1

        max_body_len += 1  # mp_sizeof_uint(TP_KEY);

        self.ensure_allocated(max_body_len)

        p = begin = &self._buf[self._length]
        p = mp_encode_map(p, body_map_sz)
        p = mp_encode_uint(p, tarantool.IPROTO_SPACE_ID)
        p = mp_encode_uint(p, space_id)
        p = mp_encode_uint(p, tarantool.IPROTO_LIMIT)
        p = mp_encode_uint(p, limit)

        if index_id > 0:
            p = mp_encode_uint(p, tarantool.IPROTO_INDEX_ID)
            p = mp_encode_uint(p, index_id)
        if offset > 0:
            p = mp_encode_uint(p, tarantool.IPROTO_OFFSET)
            p = mp_encode_uint(p, offset)
        if iterator > 0:
            p = mp_encode_uint(p, tarantool.IPROTO_ITERATOR)
            p = mp_encode_uint(p, iterator)

        p = mp_encode_uint(p, tarantool.IPROTO_KEY)
        self._length += (p - begin)
        p = self._encode_key_sequence(p, key, index.fields, False)

    cdef void encode_request_insert(self, SchemaSpace space, t) except *:
        cdef:
            char *begin
            char *p
            uint32_t body_map_sz
            uint32_t max_body_len
            uint32_t space_id

        space_id = space.sid

        body_map_sz = 2
        # Size description:
        # mp_sizeof_map(body_map_sz)
        # + mp_sizeof_uint(TP_SPACE)
        # + mp_sizeof_uint(space)
        # + mp_sizeof_uint(TP_TUPLE)
        max_body_len = 1 \
                       + 1 \
                       + 9 \
                       + 1

        self.ensure_allocated(max_body_len)

        p = begin = &self._buf[self._length]
        p = mp_encode_map(p, body_map_sz)
        p = mp_encode_uint(p, tarantool.IPROTO_SPACE_ID)
        p = mp_encode_uint(p, space_id)

        p = mp_encode_uint(p, tarantool.IPROTO_TUPLE)
        self._length += (p - begin)
        p = self._encode_key_sequence(p, t, space.fields, True)

    cdef void encode_request_delete(self, SchemaSpace space, SchemaIndex index,
                                    key) except *:
        cdef:
            char *p
            uint32_t body_map_sz
            uint32_t max_body_len
            uint32_t space_id, index_id

        space_id = space.sid
        index_id = index.iid

        body_map_sz = 2 \
                      + <uint32_t>(index_id > 0)
        # Size description:
        # mp_sizeof_map(body_map_sz)
        # + mp_sizeof_uint(TP_SPACE)
        # + mp_sizeof_uint(space)
        max_body_len = 1 \
                       + 1 \
                       + 9

        if index_id > 0:
            # mp_sizeof_uint(TP_INDEX) + mp_sizeof_uint(index)
            max_body_len += 1 + 9

        max_body_len += 1  # mp_sizeof_uint(TP_KEY);

        self.ensure_allocated(max_body_len)

        p = begin = &self._buf[self._length]
        p = mp_encode_map(p, body_map_sz)
        p = mp_encode_uint(p, tarantool.IPROTO_SPACE_ID)
        p = mp_encode_uint(p, space_id)

        if index_id > 0:
            p = mp_encode_uint(p, tarantool.IPROTO_INDEX_ID)
            p = mp_encode_uint(p, index_id)

        p = mp_encode_uint(p, tarantool.IPROTO_KEY)
        self._length += (p - begin)
        p = self._encode_key_sequence(p, key, index.fields, False)

    cdef void encode_request_update(self, SchemaSpace space, SchemaIndex index,
                                    key_tuple, list operations,
                                    bint is_upsert=False) except *:
        cdef:
            char *begin
            char *p
            uint32_t body_map_sz
            uint32_t max_body_len
            uint32_t space_id, index_id
            uint32_t key_of_tuple, key_of_operations
            TntFields fields
            bint default_fields_none

        space_id = space.sid
        index_id = index.iid

        if not is_upsert:
            key_of_tuple = tarantool.IPROTO_KEY
            key_of_operations = tarantool.IPROTO_TUPLE
            fields = index.fields
            default_fields_none = False
        else:
            key_of_tuple = tarantool.IPROTO_TUPLE
            key_of_operations = tarantool.IPROTO_OPS
            fields = space.fields
            default_fields_none = True

        body_map_sz = 3 + <uint32_t>(index_id > 0)
        # Size description:
        # mp_sizeof_map(body_map_sz)
        # + mp_sizeof_uint(TP_SPACE)
        # + mp_sizeof_uint(space)
        max_body_len = 1 \
                       + 1 \
                       + 9

        if index_id > 0:
            # + mp_sizeof_uint(TP_INDEX)
            # + mp_sizeof_uint(index)
            max_body_len += 1 + 9

        max_body_len += 1  # + mp_sizeof_uint(TP_KEY)
        max_body_len += 1  # + mp_sizeof_uint(TP_TUPLE)

        self.ensure_allocated(max_body_len)

        p = begin = &self._buf[self._length]
        p = mp_encode_map(p, body_map_sz)
        p = mp_encode_uint(p, tarantool.IPROTO_SPACE_ID)
        p = mp_encode_uint(p, space_id)

        if index_id > 0:
            p = mp_encode_uint(p, tarantool.IPROTO_INDEX_ID)
            p = mp_encode_uint(p, index_id)
        self._length += (p - begin)

        p = self._encode_uint(p, key_of_tuple)
        p = self._encode_key_sequence(p, key_tuple, fields,
                                      default_fields_none)

        p = self._encode_uint(p, key_of_operations)
        p = self._encode_update_ops(p, operations, space)

    cdef void encode_request_upsert(self, SchemaSpace space, t,
                                    list operations) except *:
        self.encode_request_update(space, space.get_index(0),
                                   t, operations, True)

    cdef void encode_request_sql(self, str query, args) except *:
        cdef:
            char *begin
            char *p
            uint32_t body_map_sz
            uint32_t max_body_len

            bytes query_temp
            char *query_str
            ssize_t query_len

        query_str = NULL
        query_len = 0

        query_temp = encode_unicode_string(query, self._encoding)
        cpython.bytes.PyBytes_AsStringAndSize(query_temp,
                                              &query_str,
                                              &query_len)
        body_map_sz = 2
        # Size description:
        # mp_sizeof_map()
        # + mp_sizeof_uint(TP_SQL_TEXT)
        # + mp_sizeof_str(query)
        # + mp_sizeof_uint(TP_SQL_BIND)
        max_body_len = 1 \
                       + 1 \
                       + mp_sizeof_str(<uint32_t>query_len) \
                       + 1

        self.ensure_allocated(max_body_len)

        p = begin = &self._buf[self._length]
        p = mp_encode_map(p, body_map_sz)
        p = mp_encode_uint(p, tarantool.IPROTO_SQL_TEXT)
        p = mp_encode_str(p, query_str, <uint32_t>query_len)

        p = mp_encode_uint(p, tarantool.IPROTO_SQL_BIND)
        self._length += (p - begin)
        # TODO: replace with custom encoder
        # TODO: need to simultaneously encode ordinal and named params
        p = self._encode_key_sequence(p, args)

    cdef void encode_request_auth(self,
                                  bytes username,
                                  bytes scramble) except *:
        cdef:
            char *begin
            char *p
            uint32_t body_map_sz
            uint32_t max_body_len

            char *username_str
            ssize_t username_len

            char *scramble_str
            ssize_t scramble_len

        cpython.bytes.PyBytes_AsStringAndSize(username,
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
                       + mp_sizeof_str(<uint32_t>username_len) \
                       + 1 \
                       + 1 \
                       + 1 + 9 \
                       + mp_sizeof_str(<uint32_t>scramble_len)

        self.ensure_allocated(max_body_len)

        p = begin = &self._buf[self._length]
        p = mp_encode_map(p, body_map_sz)
        p = mp_encode_uint(p, tarantool.IPROTO_USER_NAME)
        p = mp_encode_str(p, username_str, <uint32_t>username_len)

        p = mp_encode_uint(p, tarantool.IPROTO_TUPLE)
        p = mp_encode_array(p, 2)
        p = mp_encode_str(p, "chap-sha1", 9)
        p = mp_encode_str(p, scramble_str, <uint32_t>scramble_len)
        self._length += (p - begin)
