"""Tests for upsert operation."""

from __future__ import annotations

import pytest

import asynctnt
from asynctnt import Response
from asynctnt.exceptions import TarantoolSchemaError
from tests.conftest import TESTER_SPACE_ID, TESTER_SPACE_NAME
from tests.utils.assertions import assert_response_equal


class TestUpsert:
    """Upsert operation tests."""

    async def _fill_data(self, conn: asynctnt.Connection) -> list[list]:
        data = [
            [0, "a", 1],
            [1, "b", 0],
        ]
        for t in data:
            await conn.insert(TESTER_SPACE_ID, t)
        return data

    async def test_upsert_empty_one_assign(self, conn: asynctnt.Connection) -> None:
        data = [0, "hello2", 1, 4, "what is up"]

        res = await conn.upsert(TESTER_SPACE_ID, data, [["=", 2, 2]])
        assert isinstance(res, Response), "Got response"
        assert res.code == 0, "success"
        assert res.sync > 0, "sync > 0"
        assert_response_equal(res, [], "Body ok")

        res = await conn.select(TESTER_SPACE_ID, [0])
        assert_response_equal(res, [data], "Body ok")

    async def test_upsert_update_one_assign(self, conn: asynctnt.Connection) -> None:
        data = [0, "hello2", 1, 4, "what is up"]

        await conn.insert(TESTER_SPACE_ID, data)
        res = await conn.upsert(TESTER_SPACE_ID, data, [["=", 2, 2]])
        assert isinstance(res, Response), "Got response"
        assert res.code == 0, "success"
        assert res.sync > 0, "sync > 0"
        assert_response_equal(res, [], "Body ok")

        res = await conn.select(TESTER_SPACE_ID, [0])
        data[2] = 2
        assert_response_equal(res, [data], "Body ok")

    async def test_upsert_by_name(self, conn: asynctnt.Connection) -> None:
        data = [0, "hello2", 1, 4, "what is up"]

        await conn.upsert(TESTER_SPACE_NAME, data, [["=", 2, 2]])

        res = await conn.select(TESTER_SPACE_ID, [0])
        assert isinstance(res, Response), "Got response"
        assert res.code == 0, "success"
        assert res.sync > 0, "sync > 0"
        assert_response_equal(res, [data], "Body ok")

    async def test_upsert_by_name_no_schema(
        self, conn_no_schema: asynctnt.Connection
    ) -> None:
        with pytest.raises(TarantoolSchemaError):
            await conn_no_schema.upsert(
                TESTER_SPACE_NAME, [0, "hello", 1], [["=", 2, 2]]
            )

    async def test_upsert_dict_key(self, conn: asynctnt.Connection) -> None:
        data = {
            "f1": 0,
            "f2": "hello",
            "f3": 1,
            "f4": 2,
            "f5": 100,
        }

        res = await conn.upsert(TESTER_SPACE_ID, data, [["=", 2, 2]])
        assert_response_equal(res, [], "Body ok")

        res = await conn.select(TESTER_SPACE_ID, [0])
        assert_response_equal(res, [[0, "hello", 1, 2, 100]], "Body ok")

    async def test_usert_dict_resp_no_effect(self, conn: asynctnt.Connection) -> None:
        data = {
            "f1": 0,
            "f2": "hello",
            "f3": 1,
            "f4": 10,
            "f5": 1000,
        }

        res = await conn.upsert(TESTER_SPACE_ID, data, [["=", 2, 2]])
        assert_response_equal(res, [], "Body ok")
