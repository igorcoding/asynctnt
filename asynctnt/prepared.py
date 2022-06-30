from typing import List, Optional, Union, Dict, Any, TYPE_CHECKING

from .iproto import protocol

if TYPE_CHECKING:  # pragma: nocover
    from .api import Api


class PreparedStatement:
    __slots__ = (
        '_api', '_query', '_stmt_id',
        '_params', '_params_count'
    )

    def __init__(self, api: 'Api', query: str):
        self._api = api
        self._query = query
        self._stmt_id = None
        self._params = None
        self._params_count = 0

    @property
    def id(self) -> int:
        """
            Prepared statement id
        """
        return self._stmt_id

    @property
    def params_count(self) -> int:
        """
            Bound params count
        """
        return self._params_count

    @property
    def params(self) -> Optional[protocol.Metadata]:
        """
            Bound params metadata
        """
        return self._params

    async def prepare(self, timeout: float = -1.0) -> int:
        """
            Prepare statement

        :param timeout: request timeout
        :return: prepared statement id
        """
        resp = await self._api.prepare_iproto(self._query, timeout=timeout)
        self._stmt_id = resp.stmt_id
        self._params = resp.params
        self._params_count = resp.params_count
        return self._stmt_id

    async def execute(self,
                      args: Optional[List[Union[Dict[str, Any], Any]]] = None,
                      *,
                      parse_metadata: bool = True,
                      timeout: float = -1.0) -> protocol.Response:
        """
            Execute this prepared statement with specified args
        :param args: arguments list
        :param parse_metadata: whether to parse response metadata or not
        :param timeout: request timeout
        """
        return await self._api.execute(
            query=self._stmt_id,
            args=args,
            parse_metadata=parse_metadata,
            timeout=timeout,
        )

    async def unprepare(self, timeout: float = -1.0):
        """
            Unprepare current prepared statement
        :param timeout: request timeout
        """
        await self._api.unprepare_iproto(self._stmt_id, timeout=timeout)
        self._stmt_id = None

    async def __aenter__(self):
        """
            If used as a Context Manager `prepare()` and `unprepare()` methods
            are called automatically
        """
        if self._stmt_id is None:
            await self.prepare()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._stmt_id is not None:
            await self.unprepare()
