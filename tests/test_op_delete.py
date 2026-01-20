"""Tests for delete operation."""

from __future__ import annotations

import pytest

import asynctnt
from asynctnt import Response
from asynctnt.exceptions import TarantoolSchemaError
from asynctnt.instance import TarantoolSyncInstance
from tests.conftest import TESTER_SPACE_ID, TESTER_SPACE_NAME
from tests.utils.assertions import assert_response_equal, assert_response_equal_kv


class TestDelete:
    """Delete operation tests."""

    async def _fill_data(self, conn: asynctnt.Connection) -> list[list]:
        data = [
            [0, "a", 1, 2, "hello my darling"],
            [1, "b", 3, 4, "hello my darling, again"],
        ]
        for t in data:
            await conn.insert(TESTER_SPACE_ID, t)
        return data

    async def test_delete_one(self, conn: asynctnt.Connection) -> None:
        data = await self._fill_data(conn)

        res = await conn.delete(TESTER_SPACE_ID, [data[0][0]])
        assert isinstance(res, Response), "Got response"
        assert res.code == 0, "success"
        assert res.sync > 0, "sync > 0"
        assert_response_equal(res, [data[0]], "Body ok")

        res = await conn.select(TESTER_SPACE_ID, [0])
        assert_response_equal(res, [], "Body ok")

    async def test_delete_by_name(self, conn: asynctnt.Connection) -> None:
        data = await self._fill_data(conn)

        res = await conn.delete(TESTER_SPACE_NAME, [data[0][0]])
        assert isinstance(res, Response), "Got response"
        assert res.code == 0, "success"
        assert res.sync > 0, "sync > 0"
        assert_response_equal(res, [data[0]], "Body ok")

        res = await conn.select(TESTER_SPACE_ID, [0])
        assert_response_equal(res, [], "Body ok")

    async def test_delete_by_index_id(
        self, conn: asynctnt.Connection, tnt: TarantoolSyncInstance
    ) -> None:
        index_name = "temp_idx"
        res = tnt.command(f'make_third_index("{index_name}")')
        index_id = res[0][0]

        try:
            # Reconnect to refresh schema
            await conn.disconnect()
            await conn.connect()

            data = await self._fill_data(conn)

            res = await conn.delete(TESTER_SPACE_NAME, [data[1][2]], index=index_id)
            assert isinstance(res, Response), "Got response"
            assert res.code == 0, "success"
            assert res.sync > 0, "sync > 0"
            assert_response_equal(res, [data[1]], "Body ok")

            res = await conn.select(TESTER_SPACE_ID, [data[1][2]], index=index_id)
            assert_response_equal(res, [], "Body ok")
        finally:
            tnt.command(f"box.space.{TESTER_SPACE_NAME}.index.{index_name}:drop()")

    async def test_delete_by_index_name(
        self, conn: asynctnt.Connection, tnt: TarantoolSyncInstance
    ) -> None:
        index_name = "temp_idx"
        res = tnt.command(f'make_third_index("{index_name}")')
        index_id = res[0][0]

        try:
            # Reconnect to refresh schema
            await conn.disconnect()
            await conn.connect()

            data = await self._fill_data(conn)

            res = await conn.delete(TESTER_SPACE_NAME, [data[1][2]], index=index_name)
            assert isinstance(res, Response), "Got response"
            assert res.code == 0, "success"
            assert res.sync > 0, "sync > 0"
            assert_response_equal(res, [data[1]], "Body ok")

            res = await conn.select(TESTER_SPACE_ID, [data[1][2]], index=index_id)
            assert_response_equal(res, [], "Body ok")
        finally:
            tnt.command(f"box.space.{TESTER_SPACE_NAME}.index.{index_name}:drop()")

    async def test_delete_by_name_no_schema(
        self, conn_no_schema: asynctnt.Connection
    ) -> None:
        with pytest.raises(TarantoolSchemaError):
            await conn_no_schema.delete(TESTER_SPACE_NAME, [0])

    async def test_delete_by_index_name_no_schema(
        self, conn_no_schema: asynctnt.Connection
    ) -> None:
        with pytest.raises(TarantoolSchemaError):
            await conn_no_schema.delete(TESTER_SPACE_ID, [0], index="primary")

    async def test_delete_invalid_types(self, conn: asynctnt.Connection) -> None:
        with pytest.raises(
            TypeError,
            match="missing 2 required positional arguments: 'space' and 'key'",
        ):
            await conn.delete()

    async def test_delete_key_tuple(self, conn: asynctnt.Connection) -> None:
        # Should not raise
        await conn.delete(TESTER_SPACE_ID, (1,))

    async def test_delete_dict_key(self, conn: asynctnt.Connection) -> None:
        data = await self._fill_data(conn)

        res = await conn.delete(TESTER_SPACE_ID, {"f1": 0})
        assert_response_equal(res, [data[0]], "Body ok")

    async def test_delete_dict_resp(self, conn: asynctnt.Connection) -> None:
        data = [0, "hello", 0, 1, "wow"]
        await conn.insert(TESTER_SPACE_ID, data)

        res = await conn.delete(TESTER_SPACE_ID, [0])
        assert_response_equal_kv(
            res, [{"f1": 0, "f2": "hello", "f3": 0, "f4": 1, "f5": "wow"}]
        )
