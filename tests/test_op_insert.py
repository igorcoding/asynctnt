"""Tests for insert and replace operations."""

from __future__ import annotations

import pytest

import asynctnt
from asynctnt import Response
from asynctnt.exceptions import TarantoolSchemaError
from tests.conftest import TESTER_SPACE_ID, TESTER_SPACE_NAME
from tests.utils.assertions import assert_response_equal, assert_response_equal_kv
from tests.utils.params import get_complex_param


class TestInsert:
    """Insert operation tests."""

    async def test_insert_one(self, conn: asynctnt.Connection) -> None:
        data = [1, "hello", 1, 4, "what is up"]
        res = await conn.insert(TESTER_SPACE_ID, data)

        assert isinstance(res, Response), "Got response"
        assert res.code == 0, "success"
        assert res.sync > 0, "sync > 0"
        assert_response_equal(res, [data], "Body ok")

    async def test_insert_by_name(self, conn: asynctnt.Connection) -> None:
        data = [1, "hello", 1, 4, "what is up"]
        res = await conn.insert(TESTER_SPACE_NAME, data)

        assert isinstance(res, Response), "Got response"
        assert res.code == 0, "success"
        assert res.sync > 0, "sync > 0"
        assert_response_equal(res, [data], "Body ok")

    async def test_insert_by_name_no_schema(
        self, conn_no_schema: asynctnt.Connection
    ) -> None:
        data = [1, "hello", 1, 4, "what is up"]
        with pytest.raises(TarantoolSchemaError):
            await conn_no_schema.insert(TESTER_SPACE_NAME, data)

    async def test_insert_complex_tuple(self, conn: asynctnt.Connection) -> None:
        p, p_cmp = get_complex_param(replace_bin=False)
        data = [1, "hello", 1, 2, p]
        data_cmp = [1, "hello", 1, 2, p_cmp]

        res = await conn.insert(TESTER_SPACE_ID, data)
        assert_response_equal(res, [data_cmp], "Body ok")

    async def test_insert_replace(self, conn: asynctnt.Connection) -> None:
        data = [1, "hello", 1, 4, "what is up"]

        await conn.insert(TESTER_SPACE_ID, data)

        data = [1, "hello2", 1, 4, "what is up"]
        res = await conn.insert(TESTER_SPACE_ID, t=data, replace=True)

        assert_response_equal(res, [data], "Body ok")

    async def test_insert_invalid_types(self, conn: asynctnt.Connection) -> None:
        with pytest.raises(
            TypeError,
            match=r"missing 2 required positional arguments: 'space' and 't'",
        ):
            await conn.insert()

        with pytest.raises(
            TypeError, match=r"missing 1 required positional argument: 't'"
        ):
            await conn.insert(TESTER_SPACE_ID)

    async def test_replace(self, conn: asynctnt.Connection) -> None:
        data = [1, "hello", 1, 4, "what is up"]
        res = await conn.replace(TESTER_SPACE_ID, data)
        assert_response_equal(res, [data], "Body ok")

        data = [1, "hello2", 1, 5, "what is up"]
        res = await conn.replace(TESTER_SPACE_ID, data)
        assert_response_equal(res, [data], "Body ok")

    async def test_replace_invalid_types(self, conn: asynctnt.Connection) -> None:
        with pytest.raises(
            TypeError,
            match=r"missing 2 required positional arguments: 'space' and 't'",
        ):
            await conn.replace()

        with pytest.raises(
            TypeError, match=r"missing 1 required positional argument: 't'"
        ):
            await conn.replace(TESTER_SPACE_ID)

    async def test_insert_dict_key(self, conn: asynctnt.Connection) -> None:
        data = {
            "f1": 1,
            "f2": "hello",
            "f3": 5,
            "f4": 6,
            "f5": "hello dog",
        }
        data_cmp = [1, "hello", 5, 6, "hello dog"]
        res = await conn.insert(TESTER_SPACE_ID, data)
        assert_response_equal(res, [data_cmp], "Body ok")

    async def test_insert_dict_key_holes(self, conn: asynctnt.Connection) -> None:
        data = {
            "f1": 1,
            "f2": "hello",
            "f3": 3,
            "f4": 6,
            "f5": None,
        }
        data_cmp = [1, "hello", 3, 6, None]
        res = await conn.insert(TESTER_SPACE_ID, data)
        assert_response_equal(res, [data_cmp], "Body ok")

    async def test_insert_no_special_empty_key(self, conn: asynctnt.Connection) -> None:
        data = {
            "f1": 1,
            "f2": "hello",
            "f3": 3,
            "f4": 6,
            "f5": None,
        }
        res = await conn.insert(TESTER_SPACE_ID, data)

        with pytest.raises(KeyError):
            res[0][""]

    async def test_insert_dict_resp(self, conn: asynctnt.Connection) -> None:
        data = [0, "hello", 0, 5, "wow"]
        res = await conn.insert(TESTER_SPACE_ID, data)
        assert_response_equal_kv(
            res, [{"f1": 0, "f2": "hello", "f3": 0, "f4": 5, "f5": "wow"}]
        )

    async def test_insert_resp_extra(self, conn: asynctnt.Connection) -> None:
        data = [0, "hello", 5, 6, "help", "common", "yo"]
        res = await conn.insert(TESTER_SPACE_ID, data)
        assert_response_equal(res, [data])

    async def test_insert_bin_as_str(self, conn: asynctnt.Connection) -> None:
        # Should not raise UnicodeDecodeError
        (await conn.call("func_load_bin_str"))[0]
