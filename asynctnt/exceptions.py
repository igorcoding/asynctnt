

class TarantoolError(Exception):
    pass


class TarantoolSchemaError(TarantoolError):
    pass


class TarantoolRequestError(TarantoolError):
    pass


class TarantoolDatabaseError(TarantoolError):

    def __init__(self, code, message):
        super(TarantoolDatabaseError, self).__init__(code, message)
        self.code = code
        self.message = message


class TarantoolNetworkError(TarantoolError):
    pass


class TarantoolNotConnectedError(TarantoolNetworkError):
    pass


class TarantoolConnectionLostError(TarantoolNetworkError):
    pass
