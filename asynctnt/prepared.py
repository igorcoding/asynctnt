from typing import List, Optional, Union, Dict, Any, TYPE_CHECKING

from .iproto import protocol

if TYPE_CHECKING:
    from .connection import Connection


class PreparedStatement:
    def __init__(self, conn: 'Connection', query: str):
        self._conn = conn
        self._query = query
        self._stmt_id = None
        self._params = None
        self._params_count = 0

    @property
    def id(self) -> int:
        return self._stmt_id

    @property
    def params_count(self) -> int:
        return self._params_count

    @property
    def params(self) -> Optional[protocol.Metadata]:
        return self._params

    async def prepare(self, timeout: float = -1.0) -> int:
        resp = await self._conn.prepare_iproto(self._query, timeout=timeout)
        self._stmt_id = resp.stmt_id
        self._params = resp.params
        self._params_count = resp.params_count
        return self._stmt_id

    async def execute(self,
                      args: Optional[List[Union[Dict[str, Any], Any]]] = None,
                      *,
                      parse_metadata: bool = True,
                      timeout: float = -1.0) -> protocol.Response:
        return await self._conn.execute(
            query=self._stmt_id,
            args=args,
            parse_metadata=parse_metadata,
            timeout=timeout,
        )

    async def unprepare(self, timeout: float = -1.0):
        await self._conn.unprepare_iproto(self._stmt_id, timeout=timeout)
        self._stmt_id = None

    async def __aenter__(self):
        if self._stmt_id is None:
            await self.prepare()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._stmt_id is not None:
            await self.unprepare()
