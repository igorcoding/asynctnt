cdef class DeleteRequest(BaseRequest):
    cdef:
        SchemaIndex index
        object key
