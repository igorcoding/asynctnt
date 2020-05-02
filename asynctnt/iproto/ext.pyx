from libc cimport math

from decimal import Decimal

cdef uint32_t decimal_len(int exponent, tuple digits):
    cdef:
        uint32_t length
        uint32_t digits_len
        bint is_odd

    length = 0
    if exponent > 0:
        length += mp_sizeof_int(-exponent)
    else:
        length += mp_sizeof_uint(-exponent)

    digits_len = <uint32_t> len(digits)
    is_odd = (digits_len % 2 == 1)
    length += 1 + <uint32_t> math.ceil((is_odd + digits_len) / 2)
    return length

cdef char *decimal_encode(char *p, uint8_t sign, tuple digits, int exponent):
    cdef:
        uint8_t digit
        int i
        uint8_t byte
        uint32_t n
        uint32_t digits_len
        bint is_even
        uint32_t delta

    # encode exponent
    if exponent > 0:
        p = mp_encode_int(p, -exponent)
    else:
        p = mp_encode_uint(p, -exponent)

    digits_len = <uint32_t> len(digits)
    is_odd = (digits_len % 2 == 1)

    print('encode', exponent, digits, sign)

    # encode 1st digit
    if is_odd:
        p[0] = 0
        p += 1
        delta = 0
    else:
        byte = <uint8_t> digits[0]
        p[0] = <char> byte
        p += 1
        delta = 1

    n = <uint32_t> math.ceil((digits_len - (delta + 1)) / 2)
    for i in range(n):
        byte = 0
        byte = <uint8_t> digits[delta + i]
        byte <<= 4
        byte |= <uint8_t> digits[delta + i + 1]
        p[0] = <char> byte
        p += 1

    # encode last digit
    byte = 0
    byte = digits[digits_len - 1]
    byte <<= 4

    # encode nibble
    if sign == 1:
        byte |= 0x0d
    else:
        byte |= 0x0c

    p[0] = <char> byte
    p += 1
    return p

cdef object decimal_decode(const char ** p, uint32_t length):
    cdef:
        int exponent
        uint8_t sign
        mp_type obj_type
        const char *svp
        uint32_t digits_count
        uint32_t i, total
        uint8_t dig1, dig2

    sign = 0
    svp = &p[0][0]

    # decode exponent
    obj_type = mp_typeof(p[0][0])
    if obj_type == MP_UINT:
        exponent = -<int> mp_decode_uint(p)
    elif obj_type == MP_INT:
        exponent = -<int> mp_decode_int(p)
    else:
        raise TypeError('unexpected exponent type: {}'.format(obj_type))

    length -= (&p[0][0] - svp)
    svp = &p[0][0]

    # decode digits
    digits_count = 2 + 2 * (length - 2)
    print('digits count:', digits_count)
    digits = cpython.tuple.PyTuple_New(digits_count)

    # decode 1st digit
    dig1 = <uint8_t> svp[0]
    item = <object> <int> dig1
    cpython.Py_INCREF(item)
    cpython.tuple.PyTuple_SET_ITEM(digits, 0, item)
    print(item)

    svp += 1

    # decode digits
    total = 1
    i = 1
    for i in range(1, length - 1):
        dig2 = <uint8_t> svp[0]
        dig1 = dig2 >> 4
        dig2 &= 0x0f
        svp += 1

        item = <object> <int> dig1
        cpython.Py_INCREF(item)
        cpython.tuple.PyTuple_SET_ITEM(digits, total, item)
        total += 1
        print(item)

        item = <object> <int> dig2
        cpython.Py_INCREF(item)
        cpython.tuple.PyTuple_SET_ITEM(digits, total, item)
        total += 1
        print(item)

    # decode last digit and nibble
    dig2 = <uint8_t> svp[0]
    dig1 = dig2 >> 4
    dig2 &= 0x0f
    svp += 1

    item = <object> <int> dig1
    cpython.Py_INCREF(item)
    cpython.tuple.PyTuple_SET_ITEM(digits, total, item)
    total += 1

    sign = dig2
    if sign == 0x0a or sign == 0x0c or sign == 0x0e or sign == 0x0f:
        sign = 0
    else:
        sign = 1

    print(item, sign)

    print('length: ', length)
    p[0] += length
    print('digits', digits)

    return Decimal((<object> <int> sign, digits, <object> exponent))
