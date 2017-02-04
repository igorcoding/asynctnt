cimport cpython
cimport cython
cimport cpython.bytes
cimport cpython.list
cimport cpython.unicode

from libc.string cimport memcpy
from cpython.mem cimport PyMem_Malloc, PyMem_Realloc, PyMem_Free
from cpython cimport PyBuffer_FillInfo, PyBytes_AsString
from cpython.ref cimport PyObject, Py_DECREF
from libc.stdint cimport uint32_t, uint64_t, int64_t


cdef class Memory:
    cdef as_bytes(self):
        return cpython.PyBytes_FromStringAndSize(self.buf, self.length)

    @staticmethod
    cdef inline Memory new(char* buf, ssize_t length):
        cdef Memory mem
        mem = Memory.__new__(Memory)
        mem.buf = buf
        mem.length = length
        return mem

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
        self._encoding = 'utf-8'

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

        PyBuffer_FillInfo(
            buffer, self, self._buf, self._length,
            1,  # read-only
            flags)

    def __releasebuffer__(self, Py_buffer *buffer):
        self._view_count -= 1

    cdef inline _check_readonly(self):
        if self._view_count:
            raise BufferError('the buffer is in read-only mode')

    cdef inline len(self):
        return self._length

    cdef inline void ensure_allocated(self, ssize_t extra_length):
        cdef ssize_t new_size = extra_length + self._length

        if new_size > self._size:
            self._reallocate(new_size)

    cdef void _reallocate(self, ssize_t new_size):
        cdef char *new_buf
    
    
        print('reallocate: {}'.format(new_size))
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

    @staticmethod
    cdef WriteBuffer new(str encoding):
        cdef WriteBuffer buf
        buf = WriteBuffer.__new__(WriteBuffer)
        buf._encoding = encoding
        return buf
    
    cdef void write_header(self, uint64_t sync, tnt.tp_request_type op):
        cdef char* p = NULL
        self.ensure_allocated(HEADER_CONST_LEN)
        
        p = &self._buf[self._length]
        p = mp_encode_map(&p[5], 2)
        p = mp_encode_uint(p, tnt.TP_CODE)
        p = mp_encode_uint(p, <uint32_t>op)
        p = mp_encode_uint(p, tnt.TP_SYNC)
        p = mp_encode_uint(p, sync)
        self._length += (p - self._buf)
        
    cdef void write_length(self):
        cdef:
            char* p
        p = self._buf
        p = mp_store_u8(p, 0xce)
        p = mp_store_u32(p, self._length - 5)
        
    cdef char* _encode_nil(self, char* p):
        self.ensure_allocated(1)
        return mp_encode_nil(p)
    
    cdef char* _encode_bool(self, char* p, bint value):
        self.ensure_allocated(1)
        return mp_encode_bool(p, value)
    
    cdef char* _encode_double(self, char* p, double value):
        self.ensure_allocated(mp_sizeof_double(value))
        return mp_encode_double(p, value)
    
    cdef char* _encode_uint(self, char* p, uint64_t value):
        self.ensure_allocated(mp_sizeof_uint(value))
        return mp_encode_uint(p, value)
    
    cdef char* _encode_int(self, char* p, int64_t value):
        self.ensure_allocated(mp_sizeof_int(value))
        return mp_encode_int(p, value)
    
    cdef char* _encode_str(self, char* p, const char* str, uint32_t len):
        self.ensure_allocated(mp_sizeof_str(len))
        return mp_encode_str(p, str, len)
    
    cdef char* _encode_bin(self, char* p, const char* data, uint32_t len):
        self.ensure_allocated(mp_sizeof_bin(len))
        return mp_encode_bin(p, data, len)
    
    cdef char* _encode_array(self, char* p, uint32_t len):
        self.ensure_allocated(mp_sizeof_array(len))
        return mp_encode_array(p, len)
    
    cdef char* _encode_map(self, char* p, uint32_t len):
        self.ensure_allocated(mp_sizeof_map(len))
        return mp_encode_map(p, len)
    
    cdef char* _encode_list(self, char* p, list arr):
        cdef:
            uint32_t arr_len
            
        if arr is not None:
            arr_len = <uint32_t>cpython.list.PyList_GET_SIZE(arr)
        else:
            arr_len = 0
        p = self._encode_array(p, arr_len)
        
        if arr_len > 0:
            for item in arr:
                p = self._encode_obj(p, item)
        return p
    
    cdef char* _encode_dict(self, char* p, dict d):
        cdef:
            uint32_t d_len
            
        d_len = len(d)
        p = self._encode_map(p, d_len)
        for k, v in d.items():
            p = self._encode_obj(p, k)
            p = self._encode_obj(p, v)
        return p

    cdef char* _encode_obj(self, char* p, object o):
        cdef:
            bytes o_string_temp
            char* o_string_str
            ssize_t o_string_len
            
        # o_ptr = <PyObject*>o
            
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
        
        elif isinstance(o, str):
            o_string_temp = o.encode(self._encoding, 'strict')
            o_string_str = NULL
            o_string_len = 0
            cpython.bytes.PyBytes_AsStringAndSize(o_string_temp,
                                                  &o_string_str,
                                                  &o_string_len)
            
            p = self._encode_str(p, o_string_str, <uint32_t>o_string_len)
            return p
        
        elif isinstance(o, bytes):
            o_string_str = NULL
            o_string_len = 0
            cpython.bytes.PyBytes_AsStringAndSize(o,
                                                  &o_string_str,
                                                  &o_string_len)
            return self._encode_bin(p, o_string_str, <uint32_t>o_string_len)
        
        elif isinstance(o, list):
            return self._encode_list(p, o)
        
        elif isinstance(o, dict):
            return self._encode_dict(p, o)
        
        else:
            raise TypeError(
                'Type `{}` is not supported for encoding'.format(type(o)))
        
    cdef tnt.tnt_update_op_kind _op_type_to_kind(self, char* str, ssize_t len):
        cdef:
            char op
        if len < 0 or len > 1:
            return tnt.OP_UPD_UNKNOWN
        
        op = str[0]
        if op == b'+' \
            or op == b'-' \
            or op == b'&' \
            or op == b'^' \
            or op == b'|':
            return tnt.OP_UPD_ARITHMETIC
        elif op == b'#':
            return tnt.OP_UPD_DELETE
        elif op == b'!' \
              or op == b'=':
            return tnt.OP_UPD_INSERT_ASSIGN
        elif op == b':':
            return tnt.OP_UPD_SPLICE
        else:
            return tnt.OP_UPD_UNKNOWN
        
    cdef char* _encode_update_ops(self, char* p, list operations):
        cdef:
            uint32_t ops_len, op_len
            bytes str_temp
            char* str_c
            ssize_t str_len
            
            char* op_str_c
            ssize_t op_str_len
            
            uint32_t extra_length
            
            tnt.tnt_update_op_kind op_kind
            uint64_t field_no
            
            uint32_t splice_position, splice_offset
            
        if operations is not None:
            ops_len = <uint32_t>cpython.list.PyList_GET_SIZE(operations)
        else:
            ops_len = 0
        
        self.ensure_allocated(mp_sizeof_array(ops_len))
        p = self._encode_array(p, ops_len)
        if ops_len == 0:
            return p
        
        for operation in operations:
            if not isinstance(operation, (list, tuple)):
                raise Exception('Single operation must be a tuple or list')
            
            op_len = <uint32_t>cpython.list.PyList_GET_SIZE(operation)
            if op_len < 3:
                raise Exception('Operation length must be at least 3')
            
            # TODO: get op type and its arguments
            op_type_str = operation[0]
            if isinstance(op_type_str, str):
                str_temp = op_type_str.encode(self._encoding, 'strict')
            elif isinstance(op_type_str, bytes):
                str_temp = op_type_str
            else:
                raise Exception('Operation type must of a str or bytes type')
            
            cpython.bytes.PyBytes_AsStringAndSize(str_temp, &op_str_c, &op_str_len)
            
            op_kind = self._op_type_to_kind(op_str_c, op_str_len)
            field_no = <uint64_t>int(operation[1])
            
            if op_kind == tnt.OP_UPD_ARITHMETIC or op_kind == tnt.OP_UPD_DELETE:
                op_argument = operation[2]
                if not isinstance(op_argument, int):
                    raise Exception('int argument required for '
                                    'Arithmetic and Delete operations')
                # mp_sizeof_array(3) + mp_sizeof_str(1) + mp_sizeof_uint(field_no)
                extra_length = 1 + 2 + mp_sizeof_uint(field_no)
                self.ensure_allocated(extra_length)
                
                p = mp_encode_array(p, 3)
                p = mp_encode_str(p, op_str_c, 1)
                p = mp_encode_uint(p, field_no)
                p = self._encode_obj(p, op_argument)
            elif op_kind == tnt.OP_UPD_INSERT_ASSIGN:
                op_argument = operation[2]
                
                # mp_sizeof_array(3) + mp_sizeof_str(1) + mp_sizeof_uint(field_no)
                extra_length = 1 + 2 + mp_sizeof_uint(field_no)
                self.ensure_allocated(extra_length)
                
                p = mp_encode_array(p, 3)
                p = mp_encode_str(p, op_str_c, 1)
                p = mp_encode_uint(p, field_no)
                p = self._encode_obj(p, op_argument)
                
            elif op_kind == tnt.OP_UPD_SPLICE:
                if op_len < 5:
                    raise Exception('Splice operation must have length of 5, '
                                    'but got: {}'.format(op_len))
                
                splice_position_obj = operation[2]
                splice_offset_obj = operation[3]
                op_argument = operation[4]
                if not isinstance(splice_position_obj, int):
                    raise Exception('Splice position must be int')
                if not isinstance(splice_offset_obj, int):
                    raise Exception('Splice offset must be int')
                
                splice_position = <uint32_t>splice_position_obj
                splice_offset = <uint32_t>splice_offset_obj
                
                # mp_sizeof_array(5) + mp_sizeof_str(1) + ...
                extra_length = 1 + 2 \
                                + mp_sizeof_uint(field_no) \
                                + mp_sizeof_uint(splice_position) \
                                + mp_sizeof_uint(splice_offset)
                self.ensure_allocated(extra_length)
                
                p = mp_encode_array(p, 5)
                p = mp_encode_str(p, op_str_c, 1)
                p = mp_encode_uint(p, field_no)
                p = mp_encode_uint(p, splice_position)
                p = mp_encode_uint(p, splice_offset)
                p = self._encode_obj(p, op_argument)
            else:
                raise Exception('Unknown update operation `{}`'.format(op_type_str))
        return p

    cdef void encode_request_call(self, str func_name, list args):
        cdef:
            char* p
            uint32_t body_map_sz
            uint32_t max_body_len
            
            bytes func_name_temp
            char* func_name_str
            ssize_t func_name_len
        
        func_name_str = NULL
        func_name_len = 0
        
        func_name_temp = func_name.encode(self._encoding, 'strict')
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
        
        p = &self._buf[self._length]
        p = mp_encode_map(p, body_map_sz)
        p = mp_encode_uint(p, tnt.TP_FUNCTION)
        p = mp_encode_str(p, func_name_str, <uint32_t>func_name_len)
        
        p = mp_encode_uint(p, tnt.TP_TUPLE)
        p = self._encode_list(p, args)
        self._length += (p - self._buf)
        
    cdef void encode_request_eval(self, str expression, list args):
        cdef:
            char* p
            uint32_t body_map_sz
            uint32_t max_body_len
            
            bytes expression_temp
            char* expression_str
            ssize_t expression_len
        
        expression_str = NULL
        expression_len = 0
        
        expression_temp = expression.encode(self._encoding, 'strict')
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
        
        p = &self._buf[self._length]
        p = mp_encode_map(p, body_map_sz)
        p = mp_encode_uint(p, tnt.TP_EXPRESSION)
        p = mp_encode_str(p, expression_str, <uint32_t>expression_len)
        
        p = mp_encode_uint(p, tnt.TP_TUPLE)
        p = self._encode_list(p, args)
        self._length += (p - self._buf)
        
    cdef void encode_request_select(self, uint32_t space, uint32_t index,
                                    list key, uint64_t offset, uint64_t limit,
                                    uint32_t iterator):
        cdef:
            char* p
            uint32_t body_map_sz
            uint32_t max_body_len
        
        body_map_sz = 3 \
                      + <uint32_t>(index > 0) \
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
        
        if index > 0:
            # mp_sizeof_uint(TP_INDEX) + mp_sizeof_uint(index)
            max_body_len += 1 + 9
        if offset > 0:
            # mp_sizeof_uint(TP_OFFSET) + mp_sizeof_uint(offset)
            max_body_len += 1 + 9
        if iterator > 0:
            # mp_sizeof_uint(TP_ITERATOR) + mp_sizeof_uint(iterator)
            max_body_len += 1 + 1
            
        max_body_len += 1  # mp_sizeof_uint(TP_KEY);
        
        self.ensure_allocated(max_body_len)
        
        p = &self._buf[self._length]
        p = mp_encode_map(p, body_map_sz)
        p = mp_encode_uint(p, tnt.TP_SPACE)
        p = mp_encode_uint(p, space)
        p = mp_encode_uint(p, tnt.TP_LIMIT)
        p = mp_encode_uint(p, limit)
        
        if index > 0:
            p = mp_encode_uint(p, tnt.TP_INDEX)
            p = mp_encode_uint(p, index)
        if offset > 0:
            p = mp_encode_uint(p, tnt.TP_OFFSET)
            p = mp_encode_uint(p, offset)
        if iterator > 0:
            p = mp_encode_uint(p, tnt.TP_ITERATOR)
            p = mp_encode_uint(p, iterator)
        
        p = mp_encode_uint(p, tnt.TP_KEY)
        p = self._encode_list(p, key)
        self._length += (p - self._buf)
        
    cdef void encode_request_insert(self, uint32_t space, list t):
        cdef:
            char* p
            uint32_t body_map_sz
            uint32_t max_body_len
        
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
        
        p = &self._buf[self._length]
        p = mp_encode_map(p, body_map_sz)
        p = mp_encode_uint(p, tnt.TP_SPACE)
        p = mp_encode_uint(p, space)
        
        p = mp_encode_uint(p, tnt.TP_TUPLE)
        p = self._encode_list(p, t)
        self._length += (p - self._buf)
        
    cdef void encode_request_delete(self, uint32_t space, uint32_t index,
                                    list key):
        cdef:
            char* p
            uint32_t body_map_sz
            uint32_t max_body_len
        
        body_map_sz = 2 \
                      + <uint32_t>(index > 0)
        # Size description:
        # mp_sizeof_map(body_map_sz)
        # + mp_sizeof_uint(TP_SPACE)
        # + mp_sizeof_uint(space)
        max_body_len = 1 \
                       + 1 \
                       + 9
        
        if index > 0:
            # mp_sizeof_uint(TP_INDEX) + mp_sizeof_uint(index)
            max_body_len += 1 + 9
            
        max_body_len += 1  # mp_sizeof_uint(TP_KEY);
        
        self.ensure_allocated(max_body_len)
        
        p = &self._buf[self._length]
        p = mp_encode_map(p, body_map_sz)
        p = mp_encode_uint(p, tnt.TP_SPACE)
        p = mp_encode_uint(p, space)
        
        if index > 0:
            p = mp_encode_uint(p, tnt.TP_INDEX)
            p = mp_encode_uint(p, index)
        
        p = mp_encode_uint(p, tnt.TP_KEY)
        p = self._encode_list(p, key)
        self._length += (p - self._buf)
        
    cdef void encode_request_update(self, uint32_t space, uint32_t index,
                                    list key_tuple, list operations,
                                    uint32_t key_of_tuple=tnt.TP_KEY,
                                    uint32_t key_of_operations=tnt.TP_TUPLE):
        cdef:
            char* p
            uint32_t body_map_sz
            uint32_t max_body_len
        
        body_map_sz = 3 + <uint32_t>(index > 0)
        # Size description:
        # mp_sizeof_map(body_map_sz)
        # + mp_sizeof_uint(TP_SPACE)
        # + mp_sizeof_uint(space)
        max_body_len = 1 \
                       + 1 \
                       + 9
        
        if index > 0:
            # + mp_sizeof_uint(TP_INDEX)
            # + mp_sizeof_uint(index)
            max_body_len += 1 + 9
            
        max_body_len += 1  # + mp_sizeof_uint(TP_KEY)
        max_body_len += 1  # + mp_sizeof_uint(TP_TUPLE)
        
        self.ensure_allocated(max_body_len)
        
        p = &self._buf[self._length]
        p = mp_encode_map(p, body_map_sz)
        p = mp_encode_uint(p, tnt.TP_SPACE)
        p = mp_encode_uint(p, space)
        
        if index > 0:
            p = mp_encode_uint(p, tnt.TP_INDEX)
            p = mp_encode_uint(p, index)
        
        p = mp_encode_uint(p, key_of_tuple)
        p = self._encode_list(p, key_tuple)
        
        p = mp_encode_uint(p, key_of_operations)
        p = self._encode_update_ops(p, operations)
        
        self._length += (p - self._buf)
        
    cdef void encode_request_upsert(self, uint32_t space,
                                    list t, list operations):
        self.encode_request_update(space, 0, t, operations,
                                   tnt.TP_TUPLE, tnt.TP_OPERATIONS)
        
    
    cdef void encode_request_auth(self, bytes username, bytes scramble):
        cdef:
            char* p
            uint32_t body_map_sz
            uint32_t max_body_len
    
            char* username_str
            ssize_t username_len
            
            char* scramble_str
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
    
        p = &self._buf[self._length]
        p = mp_encode_map(p, body_map_sz)
        p = mp_encode_uint(p, tnt.TP_USERNAME)
        p = mp_encode_str(p, username_str, <uint32_t>username_len)
    
        p = mp_encode_uint(p, tnt.TP_TUPLE)
        p = mp_encode_array(p, 2)
        p = mp_encode_str(p, "chap-sha1", 9)
        p = mp_encode_str(p, scramble_str, <uint32_t>scramble_len)
        self._length += (p - self._buf)

cdef class ReadBuffer:
    cdef:
        char* buf
