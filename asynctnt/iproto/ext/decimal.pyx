from libc.stdint cimport uint32_t

from decimal import Decimal


cdef uint32_t decimal_len(int exponent, uint32_t digits_count):
    cdef:
        uint32_t length

    length = bcd_len(digits_count)
    if exponent > 0:
        length += mp_sizeof_int(-exponent)
    else:
        length += mp_sizeof_uint(-exponent)

    return length

cdef char *decimal_encode(char *p,
                          uint32_t digits_count,
                          uint8_t sign,
                          tuple digits,
                          int exponent) except NULL:
    cdef:
        int i
        uint8_t byte
        char *out
        uint32_t length

    # encode exponent
    if exponent > 0:
        p = mp_encode_int(p, -exponent)
    else:
        p = mp_encode_uint(p, -exponent)

    length = bcd_len(digits_count)

    out = &p[length - 1]
    if sign == 1:
        byte = 0x0d
    else:
        byte = 0x0c

    i = digits_count - 1
    while out >= p:
        if i >= 0:
            byte |= (<uint8_t> <object> cpython.tuple.PyTuple_GET_ITEM(digits, i)) << 4

        out[0] = byte
        byte = 0

        if i > 0:
            byte = <uint8_t> <object> cpython.tuple.PyTuple_GET_ITEM(digits, i - 1) & 0xf

        out -= 1
        i -= 2

    p = &p[length]
    return p

cdef object decimal_decode(const char ** p, uint32_t length):
    cdef:
        int exponent
        uint8_t sign
        mp_type obj_type
        const char *first
        const char *last
        uint32_t digits_count
        uint8_t nibble

    sign = 0
    first = &p[0][0]
    last = first + length - 1

    # decode exponent
    obj_type = mp_typeof(p[0][0])
    if obj_type == MP_UINT:
        exponent = -<int> mp_decode_uint(p)
    elif obj_type == MP_INT:
        exponent = -<int> mp_decode_int(p)
    else:
        raise TypeError('unexpected exponent type: {}'.format(obj_type))

    length -= (&p[0][0] - first)
    first = &p[0][0]

    while first[0] == 0:
        first += 1  # skipping leading zeros

    sign = last[0] & 0xf  # extract sign
    if sign == 0x0a or sign == 0x0c or sign == 0x0e or sign == 0x0f:
        sign = 0
    else:
        sign = 1

    # decode digits
    digits_count = (last - first) * 2 + 1
    if first[0] & 0xf0 == 0:
        digits_count -= 1  # adjust for leading zero nibble

    digits = cpython.tuple.PyTuple_New(digits_count)

    if digits_count > 0:
        while True:
            nibble = (last[0] & 0xf0) >> 4  # left nibble first
            item = <object> <int> nibble
            cpython.Py_INCREF(item)
            cpython.tuple.PyTuple_SET_ITEM(digits, digits_count - 1, item)

            digits_count -= 1
            if digits_count == 0:
                break
            last -= 1

            nibble = last[0] & 0x0f  # right nibble
            item = <object> <int> nibble
            cpython.Py_INCREF(item)
            cpython.tuple.PyTuple_SET_ITEM(digits, digits_count - 1, item)

            digits_count -= 1
            if digits_count == 0:
                break

    p[0] += length

    return Decimal((<object> <int> sign, digits, <object> exponent))
