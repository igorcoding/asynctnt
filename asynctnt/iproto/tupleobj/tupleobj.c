/* Most of this file is copied (with little modifications) from
   https://github.com/MagicStack/asyncpg/blob/master/asyncpg/protocol/record/recordobj.c
   Portions Copyright (c) PSF (and other CPython copyright holders).
   Portions Copyright (c) 2016-present MagicStack Inc.
   License:  Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0
   License: PSFL v2; see CPython/LICENSE for details.
*/

#include "tupleobj.h"


static PyObject *ttuple_iter(PyObject *);
static PyObject *ttuple_new_items_iter(PyObject *);

static AtntTupleObject *free_list[AtntTuple_MAXSAVESIZE];
static int numfree[AtntTuple_MAXSAVESIZE];


PyObject *
AtntTuple_New(PyObject *fields, Py_ssize_t size)
{
    AtntTupleObject *o;
    Py_ssize_t i;

    if (fields == Py_None) {
        fields = NULL;
    }

    if (size < 0 || (fields != NULL && !C_TntFields_CheckExact(fields))) {
        PyErr_BadInternalCall();
        return NULL;
    }

    if (size < AtntTuple_MAXSAVESIZE && (o = free_list[size]) != NULL) {
        free_list[size] = (AtntTupleObject *) o->ob_item[0];
        numfree[size]--;
        _Py_NewReference((PyObject *)o);
    }
    else {
        /* Check for overflow */
        if ((size_t)size > ((size_t)PY_SSIZE_T_MAX - sizeof(AtntTupleObject) -
                    sizeof(PyObject *)) / sizeof(PyObject *)) {
            return PyErr_NoMemory();
        }
        o = PyObject_GC_NewVar(AtntTupleObject, &AtntTuple_Type, size);
        if (o == NULL) {
            return NULL;
        }
    }

    for (i = 0; i < size; i++) {
        o->ob_item[i] = NULL;
    }

    Py_XINCREF(fields);
    o->fields = (struct C_TntFields *) fields;
    o->self_hash = -1;
    PyObject_GC_Track(o);
    return (PyObject *) o;
}


static void
ttuple_dealloc(AtntTupleObject *o)
{
    Py_ssize_t i;
    Py_ssize_t len = Py_SIZE(o);

    PyObject_GC_UnTrack(o);

    o->self_hash = -1;

    Py_CLEAR(o->fields);

    Py_TRASHCAN_SAFE_BEGIN(o)
    if (len > 0) {
        i = len;
        while (--i >= 0) {
            Py_CLEAR(o->ob_item[i]);
        }

        if (len < AtntTuple_MAXSAVESIZE &&
            numfree[len] < AtntTuple_MAXFREELIST &&
            AtntTuple_CheckExact(o))
        {
            o->ob_item[0] = (PyObject *) free_list[len];
            numfree[len]++;
            free_list[len] = o;
            goto done; /* return */
        }
    }
    Py_TYPE(o)->tp_free((PyObject *)o);
done:
    Py_TRASHCAN_SAFE_END(o)
}


static int
ttuple_traverse(AtntTupleObject *o, visitproc visit, void *arg)
{
    Py_ssize_t i;

    Py_VISIT(o->fields);

    for (i = Py_SIZE(o); --i >= 0;) {
        if (o->ob_item[i] != NULL) {
            Py_VISIT(o->ob_item[i]);
        }
    }

    return 0;
}


static Py_ssize_t
ttuple_length(AtntTupleObject *o)
{
    return Py_SIZE(o);
}


static Py_hash_t
ttuple_hash(AtntTupleObject *v)
{
    Py_uhash_t x;  /* Unsigned for defined overflow behavior. */
    Py_hash_t y;
    Py_ssize_t len;
    PyObject **p;
    Py_uhash_t mult;

    if (v->self_hash != -1) {
        return v->self_hash;
    }

    len = Py_SIZE(v);
    mult = _PyHASH_MULTIPLIER;

    x = 0x345678UL;
    p = v->ob_item;
    while (--len >= 0) {
        y = PyObject_Hash(*p++);
        if (y == -1) {
            return -1;
        }
        x = (x ^ (Py_uhash_t)y) * mult;
        /* the cast might truncate len; that doesn't change hash stability */
        mult += (Py_uhash_t)(82520UL + (size_t)len + (size_t)len);
    }
    x += 97531UL;
    if (x == (Py_uhash_t)-1) {
        x = (Py_uhash_t)-2;
    }
    v->self_hash = (Py_hash_t)x;
    return (Py_hash_t)x;
}


static PyObject *
ttuple_richcompare(PyObject *v, PyObject *w, int op)
{
    Py_ssize_t i;
    Py_ssize_t vlen, wlen;
    int v_is_tuple = 0;
    int w_is_tuple = 0;
    int comp;

    if (!AtntTuple_CheckExact(v)) {
        if (!PyTuple_Check(v)) {
            Py_RETURN_NOTIMPLEMENTED;
        }
        v_is_tuple = 1;
    }

    if (!AtntTuple_CheckExact(w)) {
        if (!PyTuple_Check(w)) {
            Py_RETURN_NOTIMPLEMENTED;
        }
        w_is_tuple = 1;
    }

#define V_ITEM(i) \
    (v_is_tuple ? (PyTuple_GET_ITEM(v, i)) : (AtntTuple_GET_ITEM(v, i)))
#define W_ITEM(i) \
    (w_is_tuple ? (PyTuple_GET_ITEM(w, i)) : (AtntTuple_GET_ITEM(w, i)))

    vlen = Py_SIZE(v);
    wlen = Py_SIZE(w);

    if (op == Py_EQ && vlen != wlen) {
        /* Checking if v == w, but len(v) != len(w): return False */
        Py_RETURN_FALSE;
    }

    if (op == Py_NE && vlen != wlen) {
        /* Checking if v != w, and len(v) != len(w): return True */
        Py_RETURN_TRUE;
    }

    /* Search for the first index where items are different.
     * Note that because tuples are immutable, it's safe to reuse
     * vlen and wlen across the comparison calls.
     */
    for (i = 0; i < vlen && i < wlen; i++) {
        comp = PyObject_RichCompareBool(V_ITEM(i), W_ITEM(i), Py_EQ);
        if (comp < 0) {
            return NULL;
        }
        if (!comp) {
            break;
        }
    }

    if (i >= vlen || i >= wlen) {
        /* No more items to compare -- compare sizes */
        int cmp;
        switch (op) {
            case Py_LT: cmp = vlen <  wlen; break;
            case Py_LE: cmp = vlen <= wlen; break;
            case Py_EQ: cmp = vlen == wlen; break;
            case Py_NE: cmp = vlen != wlen; break;
            case Py_GT: cmp = vlen >  wlen; break;
            case Py_GE: cmp = vlen >= wlen; break;
            default: return NULL; /* cannot happen */
        }
        if (cmp) {
            Py_RETURN_TRUE;
        }
        else {
            Py_RETURN_FALSE;
        }
    }

    /* We have an item that differs -- shortcuts for EQ/NE */
    if (op == Py_EQ) {
        Py_RETURN_FALSE;
    }
    if (op == Py_NE) {
        Py_RETURN_TRUE;
    }

    /* Compare the final item again using the proper operator */
    return PyObject_RichCompare(V_ITEM(i), W_ITEM(i), op);

#undef V_ITEM
#undef W_ITEM
}


static PyObject *
ttuple_item(AtntTupleObject *o, Py_ssize_t i)
{
    if (i < 0 || i >= Py_SIZE(o)) {
        PyErr_SetString(PyExc_IndexError, "TarantoolTuple index out of range");
        return NULL;
    }
    Py_INCREF(o->ob_item[i]);
    return o->ob_item[i];
}


static int
ttuple_item_by_name(AtntTupleObject *o, PyObject *item, PyObject **result)
{
    if (o->fields == NULL) {
        goto noitem;
    }

    PyObject *mapped;
    Py_ssize_t i;
    PyObject *value;

    mapped = PyObject_GetItem(o->fields->_mapping, item);
    if (mapped == NULL) {
        goto noitem;
    }

    if (!PyIndex_Check(mapped)) {
        Py_DECREF(mapped);
        goto noitem;
    }

    i = PyNumber_AsSsize_t(mapped, PyExc_IndexError);
    Py_DECREF(mapped);

    if (i < 0) {
        if (PyErr_Occurred()) {
            PyErr_Clear();
        }
        goto noitem;
    }

    value = ttuple_item(o, i);
    if (result == NULL) {
        PyErr_Clear();
        goto noitem;
    }

    *result = value;
    return 0;

noitem:
    PyErr_SetObject(PyExc_KeyError, item);
    return -1;
}


static PyObject *
ttuple_subscript(AtntTupleObject* o, PyObject* item)
{
    if (PyIndex_Check(item)) {
        Py_ssize_t i = PyNumber_AsSsize_t(item, PyExc_IndexError);
        if (i == -1 && PyErr_Occurred())
            return NULL;
        if (i < 0) {
            i += Py_SIZE(o);
        }
        return ttuple_item(o, i);
    }
    else if (PySlice_Check(item)) {
        Py_ssize_t start, stop, step, slicelength, cur, i;
        PyObject* result;
        PyObject* it;
        PyObject **src, **dest;

        if (PySlice_GetIndicesEx(
                item,
                Py_SIZE(o),
                &start, &stop, &step, &slicelength) < 0)
        {
            return NULL;
        }

        if (slicelength <= 0) {
            return PyTuple_New(0);
        }

        result = PyTuple_New(slicelength);
        if (!result) return NULL;

        src = o->ob_item;
        dest = ((PyTupleObject *)result)->ob_item;
        for (cur = start, i = 0; i < slicelength; cur += step, i++) {
            it = src[cur];
            Py_INCREF(it);
            dest[i] = it;
        }

        return result;
    }
    else {
        /* map by name */
        PyObject *result = NULL;
        if (ttuple_item_by_name(o, item, &result) < 0) {
            return NULL;
        }

        return result;
    }
}


static PyObject *
ttuple_repr(AtntTupleObject *v)
{
    Py_ssize_t i, n;
    PyObject *keys_iter = NULL;
    _PyUnicodeWriter writer;

    n = Py_SIZE(v);
    if (n == 0) {
        return PyUnicode_FromString("<TarantoolTuple>");
    }

    if (v->fields != NULL) {
        keys_iter = PyObject_GetIter(v->fields->_names);
        if (keys_iter == NULL) {
            return NULL;
        }
    }

    i = Py_ReprEnter((PyObject *)v);
    if (i != 0) {
        Py_XDECREF(keys_iter);
        return i > 0 ? PyUnicode_FromString("<TarantoolTuple ...>") : NULL;
    }

    _PyUnicodeWriter_Init(&writer);
    writer.overallocate = 1;
    writer.min_length = 12; /* <TarantoolTuple a=1> */

    if (_PyUnicodeWriter_WriteASCIIString(&writer, "<TarantoolTuple ", 16) < 0) {
        goto error;
    }

    for (i = 0; i < n; ++i) {
        PyObject *key = NULL;
        PyObject *key_repr = NULL;
        PyObject *val_repr = NULL;
        PyObject *i_obj = NULL;

        if (i > 0) {
            if (_PyUnicodeWriter_WriteChar(&writer, ' ') < 0) {
                goto error;
            }
        }

        if (Py_EnterRecursiveCall(" while getting the repr of a tarantool tuple")) {
            goto error;
        }
        val_repr = PyObject_Repr(v->ob_item[i]);
        Py_LeaveRecursiveCall();
        if (val_repr == NULL) {
            goto error;
        }

        if (keys_iter != NULL) {
            key = PyIter_Next(keys_iter);
        }

        if (key == NULL) {
            /* if no key found - fill the tail with numbers */
            if ((i_obj = PyLong_FromSsize_t(i)) == NULL) {
                goto error;
            }
            key_repr = PyObject_Str(i_obj);
            Py_DECREF(i_obj);
        } else {
            key_repr = PyObject_Str(key);
            Py_DECREF(key);
            if (key_repr == NULL) {
                Py_DECREF(val_repr);
                goto error;
            }
        }

        if (_PyUnicodeWriter_WriteStr(&writer, key_repr) < 0) {
            Py_DECREF(key_repr);
            Py_DECREF(val_repr);
            goto error;
        }
        Py_DECREF(key_repr);

        if (_PyUnicodeWriter_WriteChar(&writer, '=') < 0) {
            Py_DECREF(val_repr);
            goto error;
        }

        if (_PyUnicodeWriter_WriteStr(&writer, val_repr) < 0) {
            Py_DECREF(val_repr);
            goto error;
        }
        Py_DECREF(val_repr);
    }

    writer.overallocate = 0;
    if (_PyUnicodeWriter_WriteChar(&writer, '>') < 0) {
        goto error;
    }

    Py_XDECREF(keys_iter);
    Py_ReprLeave((PyObject *)v);
    return _PyUnicodeWriter_Finish(&writer);

error:
    Py_XDECREF(keys_iter);
    _PyUnicodeWriter_Dealloc(&writer);
    Py_ReprLeave((PyObject *)v);
    return NULL;
}



static PyObject *
ttuple_values(PyObject *o, PyObject *args)
{
    return ttuple_iter(o);
}


static PyObject *
ttuple_keys(PyObject *o, PyObject *args)
{
    if (!AtntTuple_CheckExact(o)) {
        PyErr_BadInternalCall();
        return NULL;
    }

    if (((AtntTupleObject *) o)->fields == NULL) {
        PyErr_SetString(PyExc_ValueError, "No keys for this tuple");
        return NULL;
    }

    return PyObject_GetIter(((AtntTupleObject *) o)->fields->_names);
}


static PyObject *
ttuple_items(PyObject *o, PyObject *args)
{
    if (!AtntTuple_CheckExact(o)) {
        PyErr_BadInternalCall();
        return NULL;
    }

    if (((AtntTupleObject *) o)->fields == NULL) {
        PyErr_SetString(PyExc_ValueError, "No keys for this tuple");
        return NULL;
    }

    return ttuple_new_items_iter(o);
}


static int
ttuple_contains(AtntTupleObject *o, PyObject *arg)
{
    if (!AtntTuple_CheckExact(o)) {
        PyErr_BadInternalCall();
        return -1;
    }

    if (o->fields == NULL) {
        PyErr_SetString(PyExc_ValueError, "No keys for this tuple");
        return 0;
    }

    return PySequence_Contains(o->fields->_mapping, arg);
}


static PyObject *
ttuple_get(AtntTupleObject* o, PyObject* args)
{
    PyObject *key;
    PyObject *defval = Py_None;
    PyObject *val = NULL;
    int res;

    if (!PyArg_UnpackTuple(args, "get", 1, 2, &key, &defval))
        return NULL;

    res = ttuple_item_by_name(o, key, &val);
    if (res < 0) {
        PyErr_Clear();
        Py_INCREF(defval);
        val = defval;
    }

    return val;
}


static PySequenceMethods ttuple_as_sequence = {
    (lenfunc)ttuple_length,                          /* sq_length */
    0,                                               /* sq_concat */
    0,                                               /* sq_repeat */
    (ssizeargfunc)ttuple_item,                       /* sq_item */
    0,                                               /* sq_slice */
    0,                                               /* sq_ass_item */
    0,                                               /* sq_ass_slice */
    (objobjproc)ttuple_contains,                     /* sq_contains */
};


static PyMappingMethods ttuple_as_mapping = {
    (lenfunc)ttuple_length,                          /* mp_length */
    (binaryfunc)ttuple_subscript,                    /* mp_subscript */
    0                                                /* mp_ass_subscript */
};


static PyMethodDef ttuple_methods[] = {
    {"values", (PyCFunction) ttuple_values, METH_NOARGS},
    {"keys",   (PyCFunction) ttuple_keys, METH_NOARGS},
    {"items",  (PyCFunction) ttuple_items, METH_NOARGS},
    {"get",    (PyCFunction) ttuple_get, METH_VARARGS},
    {NULL,     NULL}           /* sentinel */
};


PyTypeObject AtntTuple_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "asynctnt.TarantoolTuple",                       /* tp_name */
    sizeof(AtntTupleObject) - sizeof(PyObject *),    /* tp_basic_size */
    sizeof(PyObject *),                              /* tp_itemsize */
    (destructor)ttuple_dealloc,                      /* tp_dealloc */
    0,                                               /* tp_print */
    0,                                               /* tp_getattr */
    0,                                               /* tp_setattr */
    0,                                               /* tp_reserved */
    (reprfunc)ttuple_repr,                           /* tp_repr */
    0,                                               /* tp_as_number */
    &ttuple_as_sequence,                             /* tp_as_sequence */
    &ttuple_as_mapping,                              /* tp_as_mapping */
    (hashfunc)ttuple_hash,                           /* tp_hash */
    0,                                               /* tp_call */
    0,                                               /* tp_str */
    PyObject_GenericGetAttr,                         /* tp_getattro */
    0,                                               /* tp_setattro */
    0,                                               /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC |
        Py_TPFLAGS_BASETYPE,                         /* tp_flags */
    0,                                               /* tp_doc */
    (traverseproc)ttuple_traverse,                   /* tp_traverse */
    0,                                               /* tp_clear */
    ttuple_richcompare,                              /* tp_richcompare */
    0,                                               /* tp_weaklistoffset */
    ttuple_iter,                                     /* tp_iter */
    0,                                               /* tp_iternext */
    ttuple_methods,                                  /* tp_methods */
    0,                                               /* tp_members */
    0,                                               /* tp_getset */
    0,                                               /* tp_base */
    0,                                               /* tp_dict */
    0,                                               /* tp_descr_get */
    0,                                               /* tp_descr_set */
    0,                                               /* tp_dictoffset */
    0,                                               /* tp_init */
    0,                                               /* tp_alloc */
    0,                                               /* tp_new */
    PyObject_GC_Del,                                 /* tp_free */
};


/* TarantoolTuple Iterator */


typedef struct {
    PyObject_HEAD
    Py_ssize_t it_index;
    AtntTupleObject *it_seq; /* Set to NULL when iterator is exhausted */
} AtntTupleIterObject;


static void
ttuple_iter_dealloc(AtntTupleIterObject *it)
{
    PyObject_GC_UnTrack(it);
    Py_CLEAR(it->it_seq);
    PyObject_GC_Del(it);
}


static int
ttuple_iter_traverse(AtntTupleIterObject *it, visitproc visit, void *arg)
{
    Py_VISIT(it->it_seq);
    return 0;
}


static PyObject *
ttuple_iter_next(AtntTupleIterObject *it)
{
    AtntTupleObject *seq;
    PyObject *item;

    assert(it != NULL);
    seq = it->it_seq;
    if (seq == NULL)
        return NULL;
    assert(AtntTuple_CheckExact(seq));

    if (it->it_index < Py_SIZE(seq)) {
        item = AtntTuple_GET_ITEM(seq, it->it_index);
        ++it->it_index;
        Py_INCREF(item);
        return item;
    }

    it->it_seq = NULL;
    Py_DECREF(seq);
    return NULL;
}


static PyObject *
ttuple_iter_len(AtntTupleIterObject *it)
{
    Py_ssize_t len = 0;
    if (it->it_seq) {
        len = Py_SIZE(it->it_seq) - it->it_index;
    }
    return PyLong_FromSsize_t(len);
}


PyDoc_STRVAR(ttuple_iter_len_doc,
             "Private method returning an estimate of len(list(it)).");


static PyMethodDef ttuple_iter_methods[] = {
    {"__length_hint__", (PyCFunction)ttuple_iter_len, METH_NOARGS,
        ttuple_iter_len_doc},
    {NULL,              NULL}           /* sentinel */
};


PyTypeObject AtntTupleIter_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "TarantoolTupleIterator",                   /* tp_name */
    sizeof(AtntTupleIterObject),                /* tp_basicsize */
    0,                                          /* tp_itemsize */
    /* methods */
    (destructor)ttuple_iter_dealloc,            /* tp_dealloc */
    0,                                          /* tp_print */
    0,                                          /* tp_getattr */
    0,                                          /* tp_setattr */
    0,                                          /* tp_reserved */
    0,                                          /* tp_repr */
    0,                                          /* tp_as_number */
    0,                                          /* tp_as_sequence */
    0,                                          /* tp_as_mapping */
    0,                                          /* tp_hash */
    0,                                          /* tp_call */
    0,                                          /* tp_str */
    PyObject_GenericGetAttr,                    /* tp_getattro */
    0,                                          /* tp_setattro */
    0,                                          /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,    /* tp_flags */
    0,                                          /* tp_doc */
    (traverseproc)ttuple_iter_traverse,         /* tp_traverse */
    0,                                          /* tp_clear */
    0,                                          /* tp_richcompare */
    0,                                          /* tp_weaklistoffset */
    PyObject_SelfIter,                          /* tp_iter */
    (iternextfunc)ttuple_iter_next,             /* tp_iternext */
    ttuple_iter_methods,                        /* tp_methods */
    0,
};


static PyObject *
ttuple_iter(PyObject *seq)
{
    AtntTupleIterObject *it;

    if (!AtntTuple_CheckExact(seq)) {
        PyErr_BadInternalCall();
        return NULL;
    }
    it = PyObject_GC_New(AtntTupleIterObject, &AtntTupleIter_Type);
    if (it == NULL)
        return NULL;
    it->it_index = 0;
    Py_INCREF(seq);
    it->it_seq = (AtntTupleObject *)seq;
    PyObject_GC_Track(it);
    return (PyObject *)it;
}


/* TarantoolTuple Items Iterator */


typedef struct {
    PyObject_HEAD
    Py_ssize_t it_index;
    PyObject *it_key_iter;
    AtntTupleObject *it_seq; /* Set to NULL when iterator is exhausted */
} AtntTupleItemsObject;


static void
ttuple_items_dealloc(AtntTupleItemsObject *it)
{
    PyObject_GC_UnTrack(it);
    Py_CLEAR(it->it_key_iter);
    Py_CLEAR(it->it_seq);
    PyObject_GC_Del(it);
}


static int
ttuple_items_traverse(AtntTupleItemsObject *it, visitproc visit, void *arg)
{
    Py_VISIT(it->it_key_iter);
    Py_VISIT(it->it_seq);
    return 0;
}


static PyObject *
ttuple_items_next(AtntTupleItemsObject *it)
{
    AtntTupleObject *seq;
    PyObject *key;
    PyObject *val;
    PyObject *tup;

    assert(it != NULL);
    seq = it->it_seq;
    if (seq == NULL) {
        return NULL;
    }
    assert(AtntTuple_CheckExact(seq));
    assert(it->it_key_iter != NULL);

    key = PyIter_Next(it->it_key_iter);
    if (key == NULL) {
        /* likely it_key_iter had less items than seq has values */
        goto exhausted;
    }

    if (it->it_index < Py_SIZE(seq)) {
        val = AtntTuple_GET_ITEM(seq, it->it_index);
        ++it->it_index;
        Py_INCREF(val);
    }
    else {
        /* it_key_iter had more items than seq has values */
        Py_DECREF(key);
        goto exhausted;
    }

    tup = PyTuple_New(2);
    if (tup == NULL) {
        Py_DECREF(val);
        Py_DECREF(key);
        goto exhausted;
    }

    PyTuple_SET_ITEM(tup, 0, key);
    PyTuple_SET_ITEM(tup, 1, val);
    return tup;

exhausted:
    Py_CLEAR(it->it_key_iter);
    Py_CLEAR(it->it_seq);
    return NULL;
}


static PyObject *
ttuple_items_len(AtntTupleItemsObject *it)
{
    Py_ssize_t len = 0;
    if (it->it_seq) {
        len = Py_SIZE(it->it_seq) - it->it_index;
    }
    return PyLong_FromSsize_t(len);
}


PyDoc_STRVAR(ttuple_items_len_doc,
             "Private method returning an estimate of len(list(it())).");


static PyMethodDef ttuple_items_methods[] = {
    {"__length_hint__", (PyCFunction)ttuple_items_len, METH_NOARGS,
        ttuple_items_len_doc},
    {NULL,              NULL}           /* sentinel */
};


PyTypeObject AtntTupleItems_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "TarantoolTupleItemsIterator",              /* tp_name */
    sizeof(AtntTupleItemsObject),               /* tp_basicsize */
    0,                                          /* tp_itemsize */
    /* methods */
    (destructor)ttuple_items_dealloc,           /* tp_dealloc */
    0,                                          /* tp_print */
    0,                                          /* tp_getattr */
    0,                                          /* tp_setattr */
    0,                                          /* tp_reserved */
    0,                                          /* tp_repr */
    0,                                          /* tp_as_number */
    0,                                          /* tp_as_sequence */
    0,                                          /* tp_as_mapping */
    0,                                          /* tp_hash */
    0,                                          /* tp_call */
    0,                                          /* tp_str */
    PyObject_GenericGetAttr,                    /* tp_getattro */
    0,                                          /* tp_setattro */
    0,                                          /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,    /* tp_flags */
    0,                                          /* tp_doc */
    (traverseproc)ttuple_items_traverse,        /* tp_traverse */
    0,                                          /* tp_clear */
    0,                                          /* tp_richcompare */
    0,                                          /* tp_weaklistoffset */
    PyObject_SelfIter,                          /* tp_iter */
    (iternextfunc)ttuple_items_next,            /* tp_iternext */
    ttuple_items_methods,                       /* tp_methods */
    0,
};


static PyObject *
ttuple_new_items_iter(PyObject *seq)
{
    AtntTupleItemsObject *it;
    PyObject *key_iter;

    if (!AtntTuple_CheckExact(seq)) {
        PyErr_BadInternalCall();
        return NULL;
    }

    key_iter = PyObject_GetIter(((AtntTupleObject*)seq)->fields->_names);
    if (key_iter == NULL) {
        return NULL;
    }

    it = PyObject_GC_New(AtntTupleItemsObject, &AtntTupleItems_Type);
    if (it == NULL)
        return NULL;

    it->it_key_iter = key_iter;
    it->it_index = 0;
    Py_INCREF(seq);
    it->it_seq = (AtntTupleObject *)seq;
    PyObject_GC_Track(it);

    return (PyObject *)it;
}


PyTypeObject *
AtntTuple_InitTypes(void)
{
    if (PyType_Ready(&AtntTuple_Type) < 0) {
        return NULL;
    }

    if (PyType_Ready(&AtntTupleIter_Type) < 0) {
        return NULL;
    }

    if (PyType_Ready(&AtntTupleItems_Type) < 0) {
        return NULL;
    }

    return &AtntTuple_Type;
}
