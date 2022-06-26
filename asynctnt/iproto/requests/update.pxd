cdef class UpdateRequest(BaseRequest):
    cdef:
        SchemaIndex index
        object key
        list operations


cdef char *encode_update_ops(WriteBuffer buffer,
                             char *p, list operations,
                             SchemaSpace space) except NULL
cdef int encode_request_update(WriteBuffer buffer,
                               SchemaSpace space, SchemaIndex index,
                               key_tuple, list operations,
                               bint is_upsert) except -1
