
cdef class TntResponse:
    def __init__(self):
        self.code = 0
        self.sync = 0
        self.schema_id = -1
        self.errmsg = None
        self.body = None
        
    cdef inline has_schema_id(self):
        return self.schema_id != -1
    
    cdef inline is_error(self):
        return self.code != 0
    
    def __repr__(self):
        return '<TntResponse: code={}, sync={}>'.format(self.code, self.sync)
