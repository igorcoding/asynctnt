import copy


def get_complex_param(replace_bin=True, encoding='utf-8'):
    p = {
        'a': 1,
        'b': 2.5,
        'c': [1, 2, [4, 5], {'3': 17}],
        'd': {
            'k': 1,
            'l': (1, 2)
        },
        'e': b'1234567890'
    }
    p_copy = copy.copy(p)
    # tuples return as lists
    p_copy['d']['l'] = list(p_copy['d']['l'])

    if replace_bin:
        # For some reason Tarantool in call returns MP_STR instead of MP_BIN
        p_copy['e'] = p_copy['e'].decode(encoding)

    return p, p_copy


