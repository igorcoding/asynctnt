import asyncio
from typing import Iterator, Tuple, Optional, List, Any, Union, Dict


class Field:
    name: Optional[str]
    type: Optional[str]
    collation: Optional[str]
    is_nullable: bool
    is_autoincrement: bool
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


BodyItem = Union[TarantoolTuple, List[Any], Dict[Any, Any]]

class Response:
    errmsg: Optional[str]
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
