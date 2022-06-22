from .api import Api


class Stream(Api):
    def __init__(self):
        super().__init__()

    @property
    def stream_id(self) -> int:
        return self._db.stream_id

    async def __aenter__(self):
        await self.begin()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type and exc_val:
            await self.rollback()
        else:
            await self.commit()
