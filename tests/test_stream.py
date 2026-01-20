"""Tests for stream (MVCC transaction) operations."""

from __future__ import annotations

import asyncio
from typing import AsyncGenerator, Generator

import pytest

import asynctnt
from asynctnt.exceptions import ErrorCode, TarantoolDatabaseError
from asynctnt.instance import TarantoolSyncInstance
from tests.conftest import (
    DEFAULT_APPLUA_PATH,
    TESTER_SPACE_NAME,
    _cleanup_connection,
    create_tarantool_instance,
    ensure_version,
)
from tests.utils.assertions import assert_response_equal


@pytest.fixture(scope="class")
def tnt_mvcc_instance() -> Generator[TarantoolSyncInstance, None, None]:
    """Class-scoped MVCC-enabled Tarantool instance for stream tests."""
    instance = create_tarantool_instance(
        applua_path=DEFAULT_APPLUA_PATH,
        extra_box_cfg="memtx_use_mvcc_engine = true",
    )
    instance.start()
    yield instance
    instance.stop()


@pytest.fixture
async def conn_mvcc(
    tnt_mvcc_instance: TarantoolSyncInstance,
) -> AsyncGenerator[asynctnt.Connection, None]:
    """Connection to MVCC instance for stream tests."""
    connection = asynctnt.Connection(
        host=tnt_mvcc_instance.host,
        port=tnt_mvcc_instance.port,
        fetch_schema=True,
        auto_refetch_schema=False,
        reconnect_timeout=1 / 3,
    )
    await connection.connect()
    yield connection
    await _cleanup_connection(connection)


@pytest.mark.min_bin_version((2, 10))
class TestStream:
    """Stream (MVCC transaction) tests."""

    @ensure_version(min=(2, 10), conn="conn_mvcc")
    async def test_transaction_commit(self, conn_mvcc: asynctnt.Connection) -> None:
        s = conn_mvcc.stream()
        assert s.stream_id > 0

        await s.begin()
        data = [1, "hello", 1, 4, "what is up"]
        await s.insert(TESTER_SPACE_NAME, data)
        res = await s.select(TESTER_SPACE_NAME)
        assert_response_equal(res, [data])

        await s.commit()

        res = await conn_mvcc.select(TESTER_SPACE_NAME)
        assert_response_equal(res, [data])

    @ensure_version(min=(2, 10), conn="conn_mvcc")
    async def test_transaction_rolled_back(
        self, conn_mvcc: asynctnt.Connection
    ) -> None:
        s = conn_mvcc.stream()
        await s.begin()
        data = [1, "hello", 1, 4, "what is up"]
        await s.insert(TESTER_SPACE_NAME, data)
        res = await s.select(TESTER_SPACE_NAME)
        assert_response_equal(res, [data])

        await s.rollback()

        res = await conn_mvcc.select(TESTER_SPACE_NAME)
        assert_response_equal(res, [])

    @ensure_version(min=(2, 10), conn="conn_mvcc")
    async def test_transaction_begin_through_call(
        self, conn_mvcc: asynctnt.Connection
    ) -> None:
        s = conn_mvcc.stream()
        await s.call("box.begin")
        data = [1, "hello", 1, 4, "what is up"]
        await s.insert(TESTER_SPACE_NAME, data)
        res = await s.select(TESTER_SPACE_NAME)
        assert_response_equal(res, [data])

        await s.commit()

        res = await conn_mvcc.select(TESTER_SPACE_NAME)
        assert_response_equal(res, [data])

    @ensure_version(min=(2, 10), conn="conn_mvcc")
    async def test_transaction_commit_through_call(
        self, conn_mvcc: asynctnt.Connection
    ) -> None:
        s = conn_mvcc.stream()
        await s.begin()
        data = [1, "hello", 1, 4, "what is up"]
        await s.insert(TESTER_SPACE_NAME, data)
        res = await s.select(TESTER_SPACE_NAME)
        assert_response_equal(res, [data])

        await s.call("box.commit")

        res = await conn_mvcc.select(TESTER_SPACE_NAME)
        assert_response_equal(res, [data])

    @ensure_version(min=(2, 10), conn="conn_mvcc")
    async def test_transaction_rolled_back_through_call(
        self, conn_mvcc: asynctnt.Connection
    ) -> None:
        s = conn_mvcc.stream()
        await s.begin()
        data = [1, "hello", 1, 4, "what is up"]
        await s.insert(TESTER_SPACE_NAME, data)
        res = await s.select(TESTER_SPACE_NAME)
        assert_response_equal(res, [data])

        await s.call("box.rollback")

        res = await conn_mvcc.select(TESTER_SPACE_NAME)
        assert_response_equal(res, [])

    @ensure_version(min=(2, 10), conn="conn_mvcc")
    async def test_transaction_commit_through_sql(
        self, conn_mvcc: asynctnt.Connection
    ) -> None:
        s = conn_mvcc.stream()
        await s.execute("START TRANSACTION")
        data = [1, "hello", 1, 4, "what is up"]
        await s.insert(TESTER_SPACE_NAME, data)
        res = await s.select(TESTER_SPACE_NAME)
        assert_response_equal(res, [data])

        await s.execute("COMMIT")

        res = await conn_mvcc.select(TESTER_SPACE_NAME)
        assert_response_equal(res, [data])

    @ensure_version(min=(2, 10), conn="conn_mvcc")
    async def test_transaction_rollback_through_sql(
        self, conn_mvcc: asynctnt.Connection
    ) -> None:
        s = conn_mvcc.stream()
        await s.execute("START TRANSACTION")
        data = [1, "hello", 1, 4, "what is up"]
        await s.insert(TESTER_SPACE_NAME, data)
        res = await s.select(TESTER_SPACE_NAME)
        assert_response_equal(res, [data])

        await s.execute("ROLLBACK")

        res = await conn_mvcc.select(TESTER_SPACE_NAME)
        assert_response_equal(res, [])

    @ensure_version(min=(2, 10), conn="conn_mvcc")
    async def test_transaction_context_manager_commit(
        self, conn_mvcc: asynctnt.Connection
    ) -> None:
        data = [1, "hello", 1, 4, "what is up"]

        async with conn_mvcc.stream() as s:
            await s.insert(TESTER_SPACE_NAME, data)
            res = await s.select(TESTER_SPACE_NAME)
            assert_response_equal(res, [data])

        res = await conn_mvcc.select(TESTER_SPACE_NAME)
        assert_response_equal(res, [data])

    @ensure_version(min=(2, 10), conn="conn_mvcc")
    async def test_transaction_context_manager_rollback(
        self, conn_mvcc: asynctnt.Connection
    ) -> None:
        class ExpectedError(Exception):
            pass

        data = [1, "hello", 1, 4, "what is up"]

        try:
            async with conn_mvcc.stream() as s:
                await s.insert(TESTER_SPACE_NAME, data)
                res = await s.select(TESTER_SPACE_NAME)
                assert_response_equal(res, [data])

                raise ExpectedError("some error")
        except ExpectedError:
            pass

        res = await conn_mvcc.select(TESTER_SPACE_NAME)
        assert_response_equal(res, [])

    @ensure_version(min=(2, 10), conn="conn_mvcc")
    async def test_transaction_2_streams(self, conn_mvcc: asynctnt.Connection) -> None:
        data1 = [1, "hello", 1, 4, "what is up"]
        data2 = [2, "hi", 100, 400, "nothing match"]

        s1 = conn_mvcc.stream()
        s2 = conn_mvcc.stream()

        await s1.begin()
        await s2.begin()

        await s1.insert(TESTER_SPACE_NAME, data1)
        await s2.insert(TESTER_SPACE_NAME, data2)

        res = await s1.select(TESTER_SPACE_NAME, [1])
        assert_response_equal(res, [data1])

        res = await s2.select(TESTER_SPACE_NAME, [2])
        assert_response_equal(res, [data2])

        await s1.commit()
        await s2.commit()

        res = await conn_mvcc.select(TESTER_SPACE_NAME)
        assert_response_equal(res, [data1, data2])

    @ensure_version(min=(2, 10), conn="conn_mvcc")
    async def test_transaction_timeout(self, conn_mvcc: asynctnt.Connection) -> None:
        s = conn_mvcc.stream()
        await s.begin(tx_timeout=0.5)

        await asyncio.sleep(1.0)

        with pytest.raises(TarantoolDatabaseError) as exc:
            await s.commit()

        assert exc.value.code == ErrorCode.ER_TRANSACTION_TIMEOUT
        assert exc.value.message == "Transaction has been aborted by timeout"
