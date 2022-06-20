import asyncio
from typing import Union, Awaitable, List, Any, Tuple, Dict

from asynctnt.iproto import protocol

MethodRet = Union[Awaitable[protocol.Response], asyncio.Future]
SpaceType = Union[str, int]
IndexType = Union[str, int]
KeyType = Union[List[Any], Tuple]
TupleType = Union[List[Any], Tuple, Dict[str, Any]]
