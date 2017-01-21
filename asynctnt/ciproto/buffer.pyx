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
            raise TypeError('Type `{}` is not supported for encoding'.format(type(o)))

    cdef void encode_request_call(self, str func_name, list args):
        cdef:
            char* p
            uint32_t body_map_sz
            uint32_t max_body_len
            
            bytes func_name_temp
            char* c_func_name
            ssize_t func_name_len
        
        c_func_name = NULL
        func_name_len = 0
        
        func_name_temp = func_name.encode(self._encoding, 'strict')
        cpython.bytes.PyBytes_AsStringAndSize(func_name_temp,
                                              &c_func_name,
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
        p = mp_encode_str(p, c_func_name, <uint32_t>func_name_len)
        
        p = mp_encode_uint(p, tnt.TP_TUPLE)
        p = self._encode_list(p, args)
        self._length += (p - self._buf)

cdef class ReadBuffer:
    cdef:
        char* buf
