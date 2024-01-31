
ctypedef void (*on_unwatch_func)(Watcher watcher, object arg)

cdef class Watcher:
    cdef:
        str key_
        object cb_
        Db db_
        on_unwatch_func on_unwatch
        object on_onwatch_arg

    cdef void c_call(self, object data) except *
    cdef void c_watch(self) except *
    cdef void c_unwatch(self) except *
    cdef void c_set_on_unwatch(self, on_unwatch_func f, object arg)
