import asyncio
from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    from .api import Api

nothing = object()


class QueueWatcher:
    def __init__(self, api: "Api", key: str):
        self._api = api
        self._key = key
        self._queue = asyncio.Queue()
        self._on_unwatch_callback = None
        self._alive = True

    @property
    def key(self) -> str:
        return self._key

    def set_on_unwatch(self, cb: Callable[["Watcher"], None]):
        self._on_unwatch_callback = cb

    async def watch(self):
        await self._api.watch_iproto(self._key, self._cb)

    async def unregister(self):
        await self._api.unwatch_iproto(self._key)
        self._alive = False
        self._data = nothing
        if self._on_unwatch_callback is not None:
            self._on_unwatch_callback(self)

    async def next(self) -> Any:
        if not self._alive:
            raise StopAsyncIteration

        await self.watch()
        return await self._queue.get()

    def _cb(self, key: str, data: Any):
        self._queue.put_nowait(data)
        print("got event", key, data)

    async def __aenter__(self):
        self._alive = True
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.unregister()

    def __aiter__(self):
        return self

    async def __anext__(self) -> Any:
        return await self.next()

    def __repr__(self):
        return f"<Watcher key={self._key}>"
