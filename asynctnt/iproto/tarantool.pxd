cdef enum iproto_header_key:
    IPROTO_REQUEST_TYPE = 0x00
    IPROTO_SYNC = 0x01
    IPROTO_REPLICA_ID = 0x02
    IPROTO_LSN = 0x03
    IPROTO_TIMESTAMP = 0x04
    IPROTO_SCHEMA_VERSION = 0x05
    IPROTO_SERVER_VERSION = 0x06
    IPROTO_GROUP_ID = 0x07
    IPROTO_STREAM_ID=0x0a


cdef enum iproto_key:
    IPROTO_SPACE_ID = 0x10
    IPROTO_INDEX_ID = 0x11
    IPROTO_LIMIT = 0x12
    IPROTO_OFFSET = 0x13
    IPROTO_ITERATOR = 0x14
    IPROTO_INDEX_BASE = 0x15

    IPROTO_KEY = 0x20
    IPROTO_TUPLE = 0x21
    IPROTO_FUNCTION_NAME = 0x22
    IPROTO_USER_NAME = 0x23
    IPROTO_INSTANCE_UUID = 0x24
    IPROTO_CLUSTER_UUID = 0x25
    IPROTO_VCLOCK = 0x26
    IPROTO_EXPR = 0x27
    IPROTO_OPS = 0x28
    IPROTO_BALLOT = 0x29
    IPROTO_TUPLE_META = 0x2a
    IPROTO_OPTIONS = 0x2b

    IPROTO_DATA = 0x30
    IPROTO_ERROR_24 = 0x31
    IPROTO_METADATA = 0x32
    IPROTO_BIND_METADATA = 0x33
    IPROTO_BIND_COUNT = 0x34

    IPROTO_SQL_TEXT = 0x40
    IPROTO_SQL_BIND = 0x41
    IPROTO_SQL_INFO = 0x42
    IPROTO_STMT_ID = 0x43

    IPROTO_ERROR = 0x52
    IPROTO_VERSION = 0x54
    IPROTO_FEATURES = 0x55
    IPROTO_TIMEOUT = 0x56
    IPROTO_TXN_ISOLATION = 0x59

    IPROTO_CHUNK = 0x80


cdef enum iproto_metadata_key:
    IPROTO_FIELD_NAME = 0x00
    IPROTO_FIELD_TYPE = 0x01
    IPROTO_FIELD_COLL = 0x02
    IPROTO_FIELD_IS_NULLABLE = 0x03
    IPROTO_FIELD_IS_AUTOINCREMENT = 0x04
    IPROTO_FIELD_SPAN = 0x05


cdef enum iproto_sql_info_key:
    SQL_INFO_ROW_COUNT = 0x00
    SQL_INFO_AUTOINCREMENT_IDS = 0x01


cdef enum iproto_type:
    IPROTO_SELECT = 0x01
    IPROTO_INSERT = 0x02
    IPROTO_REPLACE = 0x03
    IPROTO_UPDATE = 0x04
    IPROTO_DELETE = 0x05
    IPROTO_CALL_16 = 0x06
    IPROTO_AUTH = 0x07
    IPROTO_EVAL = 0x08
    IPROTO_UPSERT = 0x09
    IPROTO_CALL = 0x0a
    IPROTO_EXECUTE = 0x0b
    IPROTO_PREPARE = 0x0d
    IPROTO_BEGIN = 0x0e
    IPROTO_COMMIT = 0x0f
    IPROTO_ROLLBACK = 0x10
    IPROTO_PING = 0x40
    IPROTO_ID = 0x49


cdef enum iproto_update_operation:
    IPROTO_OP_ADD = b'+'
    IPROTO_OP_SUB = b'-'
    IPROTO_OP_AND = b'&'
    IPROTO_OP_XOR = b'^'
    IPROTO_OP_OR = b'|'
    IPROTO_OP_DELETE = b'#'
    IPROTO_OP_INSERT = b'!'
    IPROTO_OP_ASSIGN = b'='
    IPROTO_OP_SPLICE = b':'


cdef enum mp_extension_type:
    MP_UNKNOWN_EXTENSION = 0
    MP_DECIMAL = 1
    MP_UUID = 2
    MP_ERROR = 3

cdef enum iproto_features:
    IPROTO_FEATURE_STREAMS = 0
    IPROTO_FEATURE_TRANSACTIONS = 1
    IPROTO_FEATURE_ERROR_EXTENSION = 2
    IPROTO_FEATURE_WATCHERS = 3

cdef enum iproto_error_fields:
    MP_ERROR_STACK = 0x00

cdef enum iproto_error_stack_fields:
    MP_ERROR_TYPE = 0x00
    MP_ERROR_FILE = 0x01
    MP_ERROR_LINE = 0x02
    MP_ERROR_MESSAGE = 0x03
    MP_ERROR_ERRNO = 0x04
    MP_ERROR_ERRCODE = 0x05
    MP_ERROR_FIELDS = 0x06
