"""Tests for ping operation."""

from __future__ import annotations

import asyncio
import os
import warnings

import pytest

import asynctnt
from asynctnt import Response
from asynctnt.exceptions import TarantoolNotConnectedError
from asynctnt.instance import TarantoolSyncInstance


class TestPing:
    """Ping operation tests."""

    SMALL_TIMEOUT = 0.00000000001

    async def test_ping_basic(self, conn: asynctnt.Connection) -> None:
        res = await conn.ping()
        assert res is not None
        assert isinstance(res, Response)
        assert res.sync > 0, "Sync is not 0"
        assert res.code == 0, "Code is 0"
        assert res.return_code == 0, "Return code is 0"

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            assert res.body is None, "No body for ping"

    async def test_ping_timeout_on_conn(self, tnt: TarantoolSyncInstance) -> None:
        connection = asynctnt.Connection(
            host=tnt.host,
            port=tnt.port,
            fetch_schema=True,
            auto_refetch_schema=False,
            reconnect_timeout=1 / 3,
            request_timeout=self.SMALL_TIMEOUT,
        )
        await connection.connect()
        try:
            assert connection.request_timeout == self.SMALL_TIMEOUT
            # Should not fail with explicit timeout override
            await connection.ping(timeout=1)
        finally:
            await connection.disconnect()

    async def test_ping_connection_lost(
        self, conn: asynctnt.Connection, tnt: TarantoolSyncInstance
    ) -> None:
        tnt.stop()
        await asyncio.sleep(0)

        try:
            os.kill(tnt.pid, 0)
        except Exception:
            pass

        with pytest.raises(TarantoolNotConnectedError):
            await conn.ping()

        tnt.start()
        await asyncio.sleep(1)

        # Should not fail after restart
        await conn.ping()

    async def test_ping_with_reconnect(self, conn: asynctnt.Connection) -> None:
        await conn.reconnect()
        res = await conn.ping()
        assert isinstance(res, Response), "Ping result"
