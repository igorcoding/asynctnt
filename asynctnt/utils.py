import asyncio
import sys
import warnings
from typing import Optional

PY_37 = sys.version_info >= (3, 7, 0)

if PY_37:
    __get_running_loop = asyncio.get_running_loop
else:
    def __get_running_loop() -> asyncio.AbstractEventLoop:
        loop = asyncio.get_event_loop()
        if not loop.is_running():
            raise RuntimeError('no running event loop')
        return loop


def get_running_loop(loop_arg: Optional[asyncio.AbstractEventLoop] = None):
    if loop_arg is not None:
        warnings.warn(
            'loop argument is deprecated and is dropped',
            DeprecationWarning,
            stacklevel=3
        )

    return __get_running_loop()
