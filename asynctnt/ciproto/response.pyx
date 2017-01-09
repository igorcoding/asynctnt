cdef class TntResponse:
    def __cinit__(self):
        self.code = 0
        self.sync = 0
        self.schema_id = -1
        self.errmsg = None
        self.body = None
    
    def __repr__(self):
        return '<TntResponse: code={}, sync={}>'.format(self.code, self.sync)
