

cdef class Watcher:
    cdef void c_call(self, object data) except *:
        self.cb_(data)

    cdef void c_watch(self) except *:
        self.db_._watch_raw(self.key_)

    cdef void c_unwatch(self) except *:
        self.db_._unwatch_raw(self.key_)
        if self.on_unwatch != NULL:
            self.on_unwatch(self, self.on_onwatch_arg)

    cdef void c_set_on_unwatch(self, on_unwatch_func f, object arg):
        self.on_unwatch = f
        self.on_onwatch_arg = arg

    @property
    def key(self):
        return self.key_

    @property
    def callback(self):
        return self.cb_

    def unwatch(self):
        self.c_unwatch()
