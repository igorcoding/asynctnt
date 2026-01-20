"""Tests for response handling."""

from __future__ import annotations

import warnings

import pytest

import asynctnt
from asynctnt import TarantoolTuple
from tests.conftest import TESTER_SPACE_ID, TESTER_SPACE_NAME
from tests.utils.assertions import assert_response_equal


class TestResponse:
    """Response handling tests."""

    async def test_response_indexing(self, conn: asynctnt.Connection) -> None:
        res = await conn.call("box.info")
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            assert len(res) == len(res.body), "len ok"
            assert res[0] == res.body[0], "value ok"

    async def test_response_iter(
        self, conn: asynctnt.Connection, fill_data: list[list]
    ) -> None:
        res = await conn.select(TESTER_SPACE_ID)

        assert len(res) == len(fill_data), "len ok"

        res_arr = []
        for el in res:
            res_arr.append(list(el))
        assert res_arr == fill_data, "list ok"

    async def test_response_tuple_iter(
        self, conn: asynctnt.Connection, fill_data: list[list]
    ) -> None:
        res = await conn.select(TESTER_SPACE_ID)
        t = res[0]

        t_list = [el for el in t]  # check iteration over tuple  # noqa: C416
        assert t_list == fill_data[0], "tuple ok"

    async def test_response_tuple_keys(
        self, conn: asynctnt.Connection, fill_data: list[list]
    ) -> None:
        res = await conn.select(TESTER_SPACE_ID)
        t = res[0]

        correct_keys = ["f1", "f2", "f3", "f4", "f5"]
        assert list(t.keys()) == correct_keys, "keys ok"

    async def test_response_tuple_values(
        self, conn: asynctnt.Connection, fill_data: list[list]
    ) -> None:
        res = await conn.select(TESTER_SPACE_ID)
        t = res[0]

        assert list(t.values()) == fill_data[0], "values ok"

    async def test_response_tuple_items(self, conn: asynctnt.Connection) -> None:
        data = [0, "hello", 5, 6, "help", "common", "yo"]
        res = await conn.insert(TESTER_SPACE_ID, data)
        t = res[0]
        d = {
            "f1": data[0],
            "f2": data[1],
            "f3": data[2],
            "f4": data[3],
            "f5": data[4],
        }

        t_dict = {k: v for k, v in t.items()}  # noqa: C416
        assert t_dict == d, "items ok"

    async def test_response_tuple_dict_extra_index(
        self, conn: asynctnt.Connection
    ) -> None:
        data = [0, "hello", 5, 6, "help", "common", "yo"]
        res = await conn.insert(TESTER_SPACE_ID, data)
        res = res[0]

        assert res[0] == data[0]
        assert res[1] == data[1]
        assert res[2] == data[2]
        assert res[3] == data[3]
        assert res[4] == data[4]
        assert res[5] == data[5]
        assert res[6] == data[6]
        assert res[-1] == data[-1]
        assert res[-3] == data[-3]

        assert res["f1"] == data[0]
        assert res["f2"] == data[1]
        assert res["f3"] == data[2]
        assert res["f4"] == data[3]
        assert res["f5"] == data[4]

    async def test_response_tuple_slice(self, conn: asynctnt.Connection) -> None:
        data = [0, "hello", 5, 6, "help", "common", "yo"]
        res = await conn.insert(TESTER_SPACE_ID, data)
        res = res[0]

        assert type(res[:3]) is tuple

        assert list(res[:3]) == data[:3]
        assert list(res[1:5]) == data[1:5]
        assert list(res[5:20]) == data[5:20]
        assert list(res[7:3:-1]) == data[7:3:-1]
        assert list(res[7:3:-2]) == data[7:3:-2]

    async def test_response_tuple_contains(self, conn: asynctnt.Connection) -> None:
        data = [0, "hello", 5, 6, "help", "common", "yo"]
        res = await conn.insert(TESTER_SPACE_ID, data)
        res = res[0]

        assert "f1" in res
        assert "f2" in res
        assert "f3" in res
        assert "f4" in res
        assert "f5" in res
        assert "f6" not in res

    async def test_response_tuple_key_error(self, conn: asynctnt.Connection) -> None:
        data = [0, "hello", 5, 6, "help", "common", "yo"]
        res = await conn.insert(TESTER_SPACE_ID, data)
        res = res[0]

        with pytest.raises(KeyError):
            # noinspection PyStatementEffect
            res["f100"]

    async def test_response_tuple_get(self, conn: asynctnt.Connection) -> None:
        data = [0, "hello", 5, 6, "help", "common", "yo"]
        res = await conn.insert(TESTER_SPACE_ID, data)
        res = res[0]

        assert res.get("f1") == 0
        assert res.get("f2") == "hello"
        assert res.get("f100") is None
        assert res.get("f100", "zz") == "zz"

    async def test_response_with_no_space_format(
        self, conn: asynctnt.Connection
    ) -> None:
        res = await conn.insert("no_schema_space", [0, "one"])

        with pytest.raises(ValueError):
            res[0].keys()

        with pytest.raises(ValueError):
            res[0].items()

    async def test_native_response_with_no_space_format(
        self, conn: asynctnt.Connection
    ) -> None:
        await conn.insert("no_schema_space", [0, "one"])

        res = await conn.select("no_schema_space")
        assert res.rowcount == 1, "count correct"
        assert isinstance(res[0], TarantoolTuple), "expecting a TarantoolTuple"
        assert_response_equal(res, [[0, "one"]], "resp ok")

        repr(res)  # Should not raise

    async def test_response_repr(self, conn: asynctnt.Connection) -> None:
        data = [0, "hello", 5, 6, "help", "common", "yo"]
        await conn.insert(TESTER_SPACE_ID, data)

        res = await conn.select("tester")
        assert res.rowcount == 1, "count correct"
        assert isinstance(res[0], TarantoolTuple), "expecting a TarantoolTuple"

        assert (
            repr(res[0])
            == "<TarantoolTuple f1=0 f2='hello' f3=5 f4=6 f5='help' 5='common' 6='yo'>"
        ), "repr ok"

    async def test_response_repr_trunc(self, conn: asynctnt.Connection) -> None:
        data = [0, "hello", 5, 6, "help", "common", "yo"]
        for _ in range(50):
            data.append("x")

        await conn.insert(TESTER_SPACE_ID, data)

        res = await conn.select("tester")
        assert res.rowcount == 1, "count correct"
        assert isinstance(res[0], TarantoolTuple), "expecting a TarantoolTuple"

        tail = []
        for i in range(7, 50):  # 50: maximum number of fields to show in repr
            tail.append(f"{i}={repr('x')}")

        assert (
            repr(res[0])
            == f"<TarantoolTuple f1=0 f2='hello' f3=5 f4=6 f5='help' 5='common' 6='yo' {' '.join(tail)} ...>"
        ), "repr ok"

    async def test_response_str(self, conn: asynctnt.Connection) -> None:
        data = [0, "hello", 5, 6, "help", "common", "yo"]
        await conn.insert(TESTER_SPACE_ID, data)

        res = await conn.select("tester")
        assert res.rowcount == 1, "count correct"
        assert isinstance(res[0], TarantoolTuple), "expecting a TarantoolTuple"

        str(res)  # Should not raise

    async def test_metadata(self, conn: asynctnt.Connection) -> None:
        assert conn.schema.id is not None
        assert conn.schema.spaces is not None
        assert TESTER_SPACE_NAME in conn.schema.spaces
        assert TESTER_SPACE_ID in conn.schema.spaces
        assert (
            conn.schema.spaces[TESTER_SPACE_NAME] is conn.schema.spaces[TESTER_SPACE_ID]
        )

        sp = conn.schema.spaces[TESTER_SPACE_NAME]
        assert TESTER_SPACE_NAME == sp.name
        assert TESTER_SPACE_ID == sp.sid
        assert "memtx" == sp.engine
        assert 4 == len(sp.indexes)
        assert "primary" in sp.indexes
        assert 0 in sp.indexes
        assert "txt" in sp.indexes
        assert 1 in sp.indexes
        assert 5 == len(sp.metadata.fields)
        assert "f1" == sp.metadata.fields[0].name
        assert "unsigned" == sp.metadata.fields[0].type
        assert "f2" == sp.metadata.fields[1].name
        assert "string" == sp.metadata.fields[1].type
        assert "f5" == sp.metadata.fields[4].name
        assert "*" == sp.metadata.fields[4].type
        assert 5 == len(sp.metadata.name_id_map)
        assert 0 == sp.metadata.name_id_map["f1"]

        idx = sp.indexes[0]
        assert 0 == idx.iid
        assert "primary" == idx.name
        assert TESTER_SPACE_ID == idx.sid
        assert "tree" == idx.index_type
        assert 1 == len(idx.metadata.fields)
        assert "f1" == idx.metadata.fields[0].name
        assert "unsigned" == idx.metadata.fields[0].type
        assert 1 == len(idx.metadata.name_id_map)
        assert 0 == idx.metadata.name_id_map["f1"]

    async def test_metadata_is_nullable(self, conn: asynctnt.Connection) -> None:
        sp_name = "test_space_with_nullable"
        await conn.eval(
            """
            local s = box.schema.space.create('%s')
            s:format({
                {name = "id", type = "unsigned"},
                {name = "name", type = "string", is_nullable = true},
            })
        """
            % (sp_name,)
        )

        try:
            # just to be sure that schema is refreshed
            await conn.refetch_schema()

            assert sp_name in conn.schema.spaces
            sp = conn.schema.spaces[sp_name]
            assert sp_name == sp.name
            assert "memtx" == sp.engine
            assert 0 == len(sp.indexes)
            assert 2 == len(sp.metadata.fields)
            assert "id" == sp.metadata.fields[0].name
            assert "unsigned" == sp.metadata.fields[0].type
            assert sp.metadata.fields[0].is_nullable is None
            assert "name" == sp.metadata.fields[1].name
            assert "string" == sp.metadata.fields[1].type
            assert sp.metadata.fields[1].is_nullable is True
            assert 2 == len(sp.metadata.name_id_map)
            assert 0 == sp.metadata.name_id_map["id"]
            assert 1 == sp.metadata.name_id_map["name"]

        finally:
            await conn.eval(
                """
                local s = box.space['%s']
                if s ~= nil then
                    s:drop()
                end
            """
                % (sp_name,)
            )
