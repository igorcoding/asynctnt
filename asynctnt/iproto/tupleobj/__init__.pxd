cimport cpython


cdef extern from "tupleobj/tupleobj.h":

    cpython.PyTypeObject *AtntTuple_InitTypes() except NULL

    int AtntTuple_CheckExact(object)
    object AtntTuple_New(object, int)
    void AtntTuple_SET_ITEM(object, int, object)

    object AtntTupleDesc_New(object, object)
