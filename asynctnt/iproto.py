from tarantool.request import RequestCall, RequestPing, RequestAuthenticate, RequestInsert, RequestSelect

all_requests = set()


def request(f):
    request_name = f.__name__
    all_requests.add(request_name)
    return f


class IProto:
    """
        Proxy to tarantool Request objects
    """
    def __init__(self):
        self._sync = 0
        
    def generate_sync(self):
        self._sync += 1
        return self._sync
    
    @request
    def ping(self):
        r = RequestPing(self)
        # print(r.sync, repr(r))
        return r.sync, bytes(r)

    @request
    def auth(self, salt, user, password):
        r = RequestAuthenticate(self, salt, user, password)
        # print(r.sync, repr(r))
        return r.sync, bytes(r)

    @request
    def call(self, func_name, args):
        r = RequestCall(self, func_name, args)
        # print(r.sync, repr(r))
        return r.sync, bytes(r)

    @request
    def select(self, space_no, index_no, key, offset, limit, iterator):
        r = RequestSelect(self, space_no, index_no, key, offset, limit, iterator)
        # print(r.sync, repr(r))
        return r.sync, bytes(r)

    @request
    def insert(self, space_no, values):
        r = RequestInsert(self, space_no, values)
        # print(r.sync, repr(r))
        return r.sync, bytes(r)
