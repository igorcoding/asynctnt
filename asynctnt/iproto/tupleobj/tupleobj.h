#ifndef ATNT_TUPLEOBJ_H
#define ATNT_TUPLEOBJ_H

#include "Python.h"
#include "protocol.h"

/* Largest ttuple to save on free list */
#define AtntTuple_MAXSAVESIZE 20

/* Maximum number of ttuples of each size to save */
#define AtntTuple_MAXFREELIST 2000


typedef struct {
    PyObject_HEAD
    PyObject *mapping;
    PyObject *keys;
} AtntTupleDescObject;


typedef struct {
    PyObject_VAR_HEAD
    Py_hash_t self_hash;
    struct C_TntFields *fields;
    PyObject *ob_item[1];

    /* ob_item contains space for 'ob_size' elements.
     * Items must normally not be NULL, except during construction when
     * the ttuple is not yet visible outside the function that builds it.
     */
} AtntTupleObject;


extern PyTypeObject AtntTuple_Type;
extern PyTypeObject AtntTupleIter_Type;
extern PyTypeObject AtntTupleItems_Type;

extern PyTypeObject AtntTupleDesc_Type;

#define AtntTuple_CheckExact(o) (Py_TYPE(o) == &AtntTuple_Type)
#define C_TntFields_CheckExact(o) (Py_TYPE(o) == &C_TntFields_Type)

#define AtntTuple_SET_ITEM(op, i, v) \
            (((AtntTupleObject *)(op))->ob_item[i] = v)
#define AtntTuple_GET_ITEM(op, i) \
            (((AtntTupleObject *)(op))->ob_item[i])

PyTypeObject *AtntTuple_InitTypes(void);
PyObject *AtntTuple_New(PyObject *, Py_ssize_t);

#endif
