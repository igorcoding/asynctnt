from .api import Api


class Stream(Api):
    def __init__(self):
        super().__init__()

    @property
    def stream_id(self) -> int:
        """
            Current stream is
        """
        return self._db.stream_id

    async def __aenter__(self):
        """
            If used as Context Manager `begin()` and `commit()`/`rollback()`
            are called automatically
        :return:
        """
        await self.begin()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """
            Normally `commit()` is called on context manager exit, but
            in the case of exception `rollback()` is called
        """
        if exc_type and exc_val:
            await self.rollback()
        else:
            await self.commit()
