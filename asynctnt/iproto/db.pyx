

cdef class Db:
    def __cinit__(self):
        self._protocol = None
        self._encoding = None

    @staticmethod
    cdef inline Db new(BaseProtocol protocol):
        cdef Db db = Db.__new__(Db)
        db._protocol = protocol
        db._encoding = protocol.encoding
        return db

    cdef inline uint64_t next_sync(self):
        return self._protocol.next_sync()

    def ping(self, timeout=0):
        return self._protocol.execute(
            request_ping(self._encoding, self.next_sync()),
            timeout
        )

    def call16(self, func_name, args=None, timeout=0):
        return self._protocol.execute(
            request_call16(self._encoding, self.next_sync(), func_name, args),
            timeout
        )

    def call(self, func_name, args=None, timeout=0):
        return self._protocol.execute(
            request_call(self._encoding, self.next_sync(), func_name, args),
            timeout
        )

    def eval(self, expression, args=None, timeout=0):
        return self._protocol.execute(
            request_eval(self._encoding, self.next_sync(), expression, args),
            timeout
        )

    def select(self, space, key=None,
                 offset=0, limit=0xffffffff, index=0, iterator=0, timeout=0):
        space = self._protocol.transform_space(space)
        index = self._protocol.transform_index(space, index)
        iterator = self._protocol.transform_iterator(iterator)

        return self._protocol.execute(
            request_select(self._encoding, self.next_sync(),
                           space, index, key, offset, limit, iterator),
            timeout
        )

    def insert(self, space, t, replace=False, timeout=0):
        space = self._protocol.transform_space(space)

        return self._protocol.execute(
            request_insert(self._encoding, self.next_sync(),
                           space, t, replace),
            timeout
        )

    def replace(self, space, t, timeout=0):
        return self.insert(space, t, replace=True, timeout=timeout)

    def delete(self, space, key, index=0, timeout=0):
        space = self._protocol.transform_space(space)
        index = self._protocol.transform_index(space, index)

        return self._protocol.execute(
            request_delete(self._encoding, self.next_sync(),
                           space, index, key),
            timeout
        )

    def update(self, space, key, operations, index=0, timeout=0):
        space = self._protocol.transform_space(space)
        index = self._protocol.transform_index(space, index)

        return self._protocol.execute(
            request_update(self._encoding, self.next_sync(),
                           space, index, key, operations),
            timeout
        )

    def upsert(self, space, t, operations, timeout=0):
        space = self._protocol.transform_space(space)

        return self._protocol.execute(
            request_upsert(self._encoding, self.next_sync(),
                           space, t, operations),
            timeout
        )
