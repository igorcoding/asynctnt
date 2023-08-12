import asyncio
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

class Field:
    name: Optional[str]
    """ Field name """

    type: Optional[str]
    """ Field type """

    collation: Optional[str]
    """ Field collation value """

    is_nullable: Optional[bool]
    """ If field may be null """

    is_autoincrement: Optional[bool]
    """ Is Autoincrement """

    span: Optional[str]

class Metadata:
    fields: List[Field]
    """ List of fields """

    name_id_map: Dict[str, int]
    """ Mapping name -> id """

class SchemaIndex:
    iid: int
    """ Index id """

    sid: int
    """ Space id """

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
    def stmt_id(self) -> int: ...
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
    def ping(self, timeout: float = -1): ...
    def call16(
        self,
        func_name: str,
        args=None,
        timeout: float = -1,
        push_subscribe: bool = False,
    ): ...
    def call(
        self,
        func_name: str,
        args=None,
        timeout: float = -1,
        push_subscribe: bool = False,
    ): ...
    def eval(
        self,
        expression: str,
        args=None,
        timeout: float = -1,
        push_subscribe: bool = False,
    ): ...
    def select(
        self,
        space,
        key=None,
        offset: int = 0,
        limit: int = 0xFFFFFFFF,
        index=0,
        iterator=0,
        timeout: float = -1,
        check_schema_change: bool = True,
    ): ...
    def insert(self, space, t, replace: bool = False, timeout: float = -1): ...
    def replace(self, space, t, timeout: float = -1): ...
    def delete(self, space, key, index=0, timeout: float = -1): ...
    def update(self, space, key, operations, index=0, timeout: float = -1): ...
    def upsert(self, space, t, operations, timeout: float = -1): ...
    def execute(
        self, query, args, parse_metadata: bool = True, timeout: float = -1
    ): ...
    def prepare(self, query, parse_metadata: bool = True, timeout: float = -1): ...
    def begin(self, isolation: int, tx_timeout: float, timeout: float = -1): ...
    def commit(self, timeout: float = -1): ...
    def rollback(self, timeout: float = -1): ...

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
