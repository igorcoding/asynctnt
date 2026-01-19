#ifndef ATNT_TUPLEOBJ_H
#define ATNT_TUPLEOBJ_H

#include "Python.h"
#include "protocol.h"

#ifdef __cplusplus
extern "C" {
#endif

#if defined(PYPY_VERSION)
#  define CPy_TRASHCAN_BEGIN(op, dealloc) do {} while(0);
#  define CPy_TRASHCAN_END(op) do {} while(0);
#else
#  define CPy_TRASHCAN_BEGIN(op, dealloc) Py_TRASHCAN_BEGIN(op, dealloc)
#  define CPy_TRASHCAN_END(op) Py_TRASHCAN_END

/*
 * PyUnicodeWriter compatibility macros for Python 3.14+
 * Python 3.14 introduced public PyUnicodeWriter API, deprecating private _PyUnicodeWriter.
 * These macros are only defined for CPython (not PyPy).
 */
#if PY_VERSION_HEX >= 0x030E0000  /* Python 3.14+ */
#  define ATNT_UW_DECL(name)              PyUnicodeWriter *name
#  define ATNT_UW_CREATE(name, len)       name = PyUnicodeWriter_Create(len)
#  define ATNT_UW_CREATE_FAILED(name)     (name == NULL)
#  define ATNT_UW_REF(name)               name
#  define ATNT_UW_WRITE_ASCII             PyUnicodeWriter_WriteASCII
#  define ATNT_UW_WRITE_CHAR              PyUnicodeWriter_WriteChar
#  define ATNT_UW_WRITE_STR               PyUnicodeWriter_WriteStr
#  define ATNT_UW_FINISH                  PyUnicodeWriter_Finish
#  define ATNT_UW_DISCARD                 PyUnicodeWriter_Discard
#else  /* Python < 3.14 */
#  define ATNT_UW_DECL(name)              _PyUnicodeWriter name
#  define ATNT_UW_CREATE(name, len)       do { _PyUnicodeWriter_Init(&name); name.overallocate = 1; name.min_length = len; } while(0)
#  define ATNT_UW_CREATE_FAILED(name)     (0)  /* _PyUnicodeWriter_Init doesn't fail */
#  define ATNT_UW_REF(name)               &name
#  define ATNT_UW_WRITE_ASCII             _PyUnicodeWriter_WriteASCIIString
#  define ATNT_UW_WRITE_CHAR              _PyUnicodeWriter_WriteChar
#  define ATNT_UW_WRITE_STR               _PyUnicodeWriter_WriteStr
#  define ATNT_UW_FINISH                  _PyUnicodeWriter_Finish
#  define ATNT_UW_DISCARD                 _PyUnicodeWriter_Dealloc
#endif

#endif /* !PYPY_VERSION */

/*
 * PyHASH_MULTIPLIER compatibility macro for Python 3.13+
 * Python 3.13 introduced public PyHASH_MULTIPLIER, replacing private _PyHASH_MULTIPLIER.
 */
#if PY_VERSION_HEX >= 0x030D0000  /* Python 3.13+ */
#  define ATNT_HASH_MULTIPLIER PyHASH_MULTIPLIER
#else
#  define ATNT_HASH_MULTIPLIER _PyHASH_MULTIPLIER
#endif

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
    struct C_Metadata *metadata;
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
#define C_Metadata_CheckExact(o) (Py_TYPE(o) == &C_Metadata_Type)

#define AtntTuple_SET_ITEM(op, i, v) \
            (((AtntTupleObject *)(op))->ob_item[i] = v)
#define AtntTuple_GET_ITEM(op, i) \
            (((AtntTupleObject *)(op))->ob_item[i])

PyTypeObject *AtntTuple_InitTypes(void);
PyObject *AtntTuple_New(PyObject *, Py_ssize_t);

#ifdef __cplusplus
}
#endif

#endif
