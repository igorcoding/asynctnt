cdef enum tnt_header_key:
	TP_CODE = 0x00
	TP_SYNC = 0x01
	TP_SERVER_ID = 0x02
	TP_LSN = 0x03
	TP_TIMESTAMP = 0x04
	TP_SCHEMA_ID = 0x05


cdef enum tnt_body_key_t:
	TP_SPACE = 0x10
	TP_INDEX = 0x11
	TP_LIMIT = 0x12
	TP_OFFSET = 0x13
	TP_ITERATOR = 0x14
	TP_KEY = 0x20
	TP_TUPLE = 0x21
	TP_FUNCTION = 0x22
	TP_USERNAME = 0x23
	TP_EXPRESSION = 0x27
	TP_OPERATIONS = 0x28


cdef enum tnt_response_key_t:
	TP_DATA = 0x30
	TP_ERROR = 0x31
