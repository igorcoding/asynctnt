import asyncio
from typing import Iterator, Tuple, Optional, List, Any, Union, Dict


class Field:
    name: Optional[str]
    type: Optional[str]
    collation: Optional[str]
    is_nullable: Optional[bool]
    is_autoincrement: Optional[bool]
    span: Optional[str]


class Metadata:
    fields: List[Field]
    name_id_map: Dict[str, int]


class SchemaIndex:
    iid: int
    sid: int
    name: Optional[str]
    index_type: Optional[str]
    unique: Optional[bool]
    metadata: Optional[Metadata]


class SchemaSpace:
    sid: int
    owner: int
    name: Optional[str]
    engine: Optional[str]
    field_count: int
    flags: Optional[Any]
    metadata: Optional[Metadata]
    indexes: Dict[Union[int, str], SchemaIndex]


class Schema:
    id: int
    spaces: Dict[Union[str, int], SchemaSpace]


class TarantoolTuple:
    def __repr__(self) -> str: ...

    def __index__(self, i: int) -> Any: ...

    def __len__(self) -> int: ...

    def __contains__(self, item: str) -> bool: ...

    def __getitem__(self, item: Union[int, str, slice]) -> Any: ...

    def keys(self) -> Iterator[str]: ...

    def values(self) -> Iterator[Any]: ...

    def items(self) -> Iterator[Tuple[str, Any]]: ...

    def get(self, item: str) -> Optional[Any]: ...

    def __iter__(self): ...

    def __next__(self): ...


class IProtoErrorStackFrame:
    error_type: str
    file: str
    line: int
    message: str
    err_no: int
    code: int
    fields: Dict[str, Any]


class IProtoError:
    trace: List[IProtoErrorStackFrame]


BodyItem = Union[TarantoolTuple, List[Any], Dict[Any, Any], Any]


class Response:
    errmsg: Optional[str]
    error: Optional[IProtoError]
    encoding: bytes
    stmt_id: Optional[int]
    autoincrement_ids: Optional[List[int]]
    body: Optional[List[BodyItem]]
    metadata: Optional[Metadata]
    params: Optional[Metadata]
    params_count: int

    @property
    def sync(self) -> int: ...

    @property
    def code(self) -> int: ...

    @property
    def return_code(self) -> int: ...

    @property
    def schema_id(self) -> int: ...

    @property
    def rowcount(self) -> int: ...

    def done(self) -> bool: ...

    def __len__(self) -> int: ...

    def __getitem__(self, i) -> BodyItem: ...

    def __iter__(self): ...


class PushIterator:
    def __init__(self, fut: asyncio.Future): ...

    def __iter__(self): ...

    def __next__(self): ...

    def __aiter__(self): ...

    async def __anext__(self): ...

    @property
    def response(self) -> Response: ...


class Db:
    @property
    def stream_id(self) -> int: ...

    def set_stream_id(self, stream_id: int): ...

    def ping(self, timeout=-1): ...

    def call16(self, func_name, args=None, timeout=-1, push_subscribe=False): ...

    def call(self, func_name, args=None, timeout=-1, push_subscribe=False): ...

    def eval(self, expression, args=None, timeout=-1, push_subscribe=False): ...

    def select(self, space, key=None,
               offset=0, limit=0xffffffff, index=0, iterator=0,
               timeout=-1, check_schema_change=True): ...

    def insert(self, space, t, replace=False,
               timeout=-1): ...

    def replace(self, space, t, timeout=-1): ...

    def delete(self, space, key, index=0, timeout=-1): ...

    def update(self, space, key, operations, index=0, timeout=-1): ...

    def upsert(self, space, t, operations, timeout=-1): ...

    def execute(self, query, args, parse_metadata=True, timeout=-1): ...

    def prepare(self, query, parse_metadata=True, timeout=-1): ...

    def begin(self, isolation: int, tx_timeout: float, timeout=-1): ...

    def commit(self, timeout=-1): ...

    def rollback(self, timeout=-1): ...


class Protocol:
    @property
    def schema_id(self) -> int: ...

    @property
    def schema(self) -> Schema: ...

    def create_db(self, gen_stream_id: bool = False) -> Db: ...

    def get_common_db(self) -> Db: ...

    def refetch_schema(self) -> asyncio.Future: ...

    def is_connected(self) -> bool: ...

    def is_fully_connected(self) -> bool: ...

    def get_version(self) -> tuple: ...
