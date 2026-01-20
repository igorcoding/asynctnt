"""Tests for select operation."""

from __future__ import annotations

import pytest

import asynctnt
from asynctnt import Iterator, Response
from asynctnt.exceptions import TarantoolSchemaError
from tests.conftest import TESTER_SPACE_ID, TESTER_SPACE_NAME
from tests.utils.assertions import assert_response_equal, assert_response_equal_kv
from tests.utils.params import get_complex_param


class TestSelect:
    """Select operation tests."""

    async def _fill_data(
        self, conn: asynctnt.Connection, count: int = 3, space: int | str | None = None
    ) -> list[list]:
        space = space or TESTER_SPACE_ID
        data = []
        for i in range(count):
            t = [i, str(i), 1, 2, "something"]
            data.append(t)
            await conn.insert(space, t)
        return data

    async def _fill_data_dict(
        self, conn: asynctnt.Connection, count: int = 3
    ) -> list[dict]:
        data = []
        for i in range(count):
            t = {
                "f1": i,
                "f2": str(i),
                "f3": 1,
                "f4": 2,
                "f5": "something",
            }
            result = await conn.insert(TESTER_SPACE_ID, t)
            data.append(dict(result[0]))
        return data

    async def test_select_by_id_empty_space(self, conn: asynctnt.Connection) -> None:
        res = await conn.select(TESTER_SPACE_ID)

        assert isinstance(res, Response), "Got response"
        assert res.code == 0, "success"
        assert res.sync > 0, "sync > 0"
        assert_response_equal(res, [], "Body ok")

    async def test_select_by_id_non_empty_space(
        self, conn: asynctnt.Connection
    ) -> None:
        data = await self._fill_data(conn)

        res = await conn.select(TESTER_SPACE_ID)

        assert isinstance(res, Response), "Got response"
        assert res.code == 0, "success"
        assert res.sync > 0, "sync > 0"
        assert_response_equal(res, data, "Body ok")

    async def test_select_by_name_space_empty(self, conn: asynctnt.Connection) -> None:
        res = await conn.select(TESTER_SPACE_NAME)

        assert isinstance(res, Response), "Got response"
        assert res.code == 0, "success"
        assert res.sync > 0, "sync > 0"
        assert_response_equal(res, [], "Body ok")

    async def test_select_by_name_non_empty_space(
        self, conn: asynctnt.Connection
    ) -> None:
        data = await self._fill_data(conn)

        res = await conn.select(TESTER_SPACE_NAME)

        assert isinstance(res, Response), "Got response"
        assert res.code == 0, "success"
        assert res.sync > 0, "sync > 0"
        assert_response_equal(res, data, "Body ok")

    async def test_select_by_index_id(self, conn: asynctnt.Connection) -> None:
        data = await self._fill_data(conn)

        res = await conn.select(TESTER_SPACE_ID, index=1)

        assert isinstance(res, Response), "Got response"
        assert res.code == 0, "success"
        assert res.sync > 0, "sync > 0"
        assert_response_equal(res, data, "Body ok")

    async def test_select_by_index_name(self, conn: asynctnt.Connection) -> None:
        data = await self._fill_data(conn)

        res = await conn.select(TESTER_SPACE_ID, index="txt")

        assert isinstance(res, Response), "Got response"
        assert res.code == 0, "success"
        assert res.sync > 0, "sync > 0"
        assert_response_equal(res, data, "Body ok")

    async def test_select_by_id_no_schema(
        self, conn_no_schema: asynctnt.Connection
    ) -> None:
        await conn_no_schema.select(TESTER_SPACE_ID)

    async def test_select_by_name_no_schema(
        self, conn_no_schema: asynctnt.Connection
    ) -> None:
        with pytest.raises(TarantoolSchemaError):
            await conn_no_schema.select(TESTER_SPACE_NAME)

    async def test_select_by_index_id_no_schema(
        self, conn_no_schema: asynctnt.Connection
    ) -> None:
        await conn_no_schema.select(TESTER_SPACE_ID, index=1)

    async def test_select_by_index_name_no_schema(
        self, conn_no_schema: asynctnt.Connection
    ) -> None:
        with pytest.raises(TarantoolSchemaError):
            await conn_no_schema.select(TESTER_SPACE_NAME, index="txt")

    async def test_select_by_key_one_item(self, conn: asynctnt.Connection) -> None:
        data = await self._fill_data(conn)

        res = await conn.select(TESTER_SPACE_NAME, [1])
        assert_response_equal(res, [data[1]], "Body ok")

    async def test_select_by_key_multiple_items_index(
        self, conn: asynctnt.Connection
    ) -> None:
        data = await self._fill_data(conn)
        next_id = data[-1][0] + 1
        next_txt = data[-1][1]
        await conn.insert(TESTER_SPACE_ID, [next_id, next_txt, 1, 2, "text"])
        data.append([next_id, next_txt, 1, 2, "text"])

        res = await conn.select(TESTER_SPACE_NAME, [next_txt], index="txt")
        assert_response_equal(res, data[len(data) - 2 :], "Body ok")

    async def test_select_limit(self, conn: asynctnt.Connection) -> None:
        data = await self._fill_data(conn)

        res = await conn.select(TESTER_SPACE_NAME, limit=1)
        assert_response_equal(res, [data[0]], "Body ok")

    async def test_select_limit_offset(self, conn: asynctnt.Connection) -> None:
        data = await self._fill_data(conn, 4)

        res = await conn.select(TESTER_SPACE_NAME, limit=1, offset=2)
        assert_response_equal(res, [data[2]], "Body ok")

    async def test_select_iterator_class(self, conn: asynctnt.Connection) -> None:
        data = await self._fill_data(conn, 4)

        res = await conn.select(TESTER_SPACE_NAME, iterator=Iterator.GE)
        assert_response_equal(res, data, "Body ok")

        res = await conn.select(TESTER_SPACE_NAME, iterator=Iterator.LE)
        assert_response_equal(res, list(reversed(data)), "Body ok")

    async def test_select_iterator_int(self, conn: asynctnt.Connection) -> None:
        data = await self._fill_data(conn, 4)

        res = await conn.select(TESTER_SPACE_NAME, iterator=4)
        assert_response_equal(res, list(reversed(data)), "Body ok")

    async def test_select_iterator_str(self, conn: asynctnt.Connection) -> None:
        data = await self._fill_data(conn, 4)

        res = await conn.select(TESTER_SPACE_NAME, iterator="LE")
        assert_response_equal(res, list(reversed(data)), "Body ok")

    async def test_select_complex(self, conn: asynctnt.Connection) -> None:
        p, p_cmp = get_complex_param(replace_bin=False)
        data = [1, "hello2", 1, 4, p_cmp]

        await conn.insert(TESTER_SPACE_ID, data)

        res = await conn.select(TESTER_SPACE_ID)
        assert_response_equal(res, [data], "Body ok")

    async def test_select_all_params(self, conn: asynctnt.Connection) -> None:
        data = await self._fill_data(conn, 10)

        res = await conn.select(
            TESTER_SPACE_NAME,
            index="primary",
            limit=2,
            offset=1,
            iterator=Iterator.LE,
        )
        assert_response_equal(res, list(reversed(data))[1:3], "Body ok")

    async def test_select_all_by_hash_index(self, conn: asynctnt.Connection) -> None:
        data = await self._fill_data(conn, 4, space="no_schema_space")

        res = await conn.select("no_schema_space", index="primary_hash")
        assert_response_equal(
            sorted(res, key=lambda t: t[0]), sorted(data, key=lambda t: t[0]), "Body ok"
        )

    async def test_select_key_tuple(self, conn: asynctnt.Connection) -> None:
        # Should not raise
        await conn.select(TESTER_SPACE_ID, (1,))

    async def test_select_invalid_types(self, conn: asynctnt.Connection) -> None:
        with pytest.raises(
            TypeError, match=r"missing 1 required positional argument: 'space'"
        ):
            await conn.select()

        with pytest.raises(
            TypeError, match=r"sequence must be either list, tuple or dict"
        ):
            await conn.select(TESTER_SPACE_ID, 1)

        with pytest.raises(TypeError, match=r"Index must be either str or int, got"):
            await conn.select(TESTER_SPACE_ID, [1], index=[1, 2])

        with pytest.raises(TypeError, match=r"an integer is required"):
            await conn.select(TESTER_SPACE_ID, [1], index=1, limit="hello")

        with pytest.raises(TypeError, match=r"an integer is required"):
            await conn.select(TESTER_SPACE_ID, [1], index=1, limit=1, offset="hello")

        with pytest.raises(TypeError, match=r"Iterator is of unsupported type"):
            await conn.select(
                TESTER_SPACE_ID, [1], index=1, limit=1, offset=1, iterator=[1, 2]
            )

    async def test_select_dict_key(self, conn: asynctnt.Connection) -> None:
        data = await self._fill_data(conn)
        res = await conn.select(TESTER_SPACE_ID, {"f1": data[0][0]})
        assert_response_equal(res, [data[0]], "Body ok")

    async def test_select_dict_key_wrong_field(self, conn: asynctnt.Connection) -> None:
        data = await self._fill_data(conn)
        res = await conn.select(TESTER_SPACE_ID, {"f2": data[0][0]})
        assert_response_equal(res, data, "Body ok")

    async def test_select_dict_key_other_index(self, conn: asynctnt.Connection) -> None:
        data = await self._fill_data(conn)
        res = await conn.select(TESTER_SPACE_ID, {"f2": data[0][1]}, index="txt")
        assert_response_equal(res, [data[0]], "Body ok")

    async def test_select_dict_resp(self, conn: asynctnt.Connection) -> None:
        data = await self._fill_data_dict(conn)
        res = await conn.select(TESTER_SPACE_ID, [])
        assert_response_equal_kv(res, data)
