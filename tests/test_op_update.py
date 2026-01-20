"""Tests for update operation."""

from __future__ import annotations

from decimal import Decimal

import pytest

import asynctnt
from asynctnt import Response
from asynctnt.exceptions import ErrorCode, TarantoolDatabaseError, TarantoolSchemaError
from asynctnt.instance import TarantoolSyncInstance
from tests.conftest import TESTER_SPACE_ID, TESTER_SPACE_NAME, ensure_version
from tests.utils.assertions import assert_response_equal, assert_response_equal_kv


class TestUpdate:
    """Update operation tests."""

    async def _fill_data(self, conn: asynctnt.Connection) -> list[list]:
        data = [
            [0, "a", 1, 5, "data1"],
            [1, "b", 8, 6, "data2"],
            [2, "c", 10, 12, "data3", "extra_field"],
            [3, "d", 14, 16, "data4", Decimal("12.3"), 12.5],
            [
                4,
                "e",
                18,
                20,
                {
                    "tree1": {
                        "tree11": {
                            "key1": "value1",
                        }
                    }
                },
            ],
        ]
        for t in data:
            await conn.insert(TESTER_SPACE_ID, t)

        return data

    async def test_update_one_assign(self, conn: asynctnt.Connection) -> None:
        data = await self._fill_data(conn)

        res = await conn.update(TESTER_SPACE_ID, [1], [["=", 2, 2]])
        assert isinstance(res, Response), "Got response"
        assert res.code == 0, "success"
        assert res.sync > 0, "sync > 0"

        data[1][2] = 2
        assert_response_equal(res, [data[1]], "Body ok")

    @pytest.mark.min_bin_version((2, 3))
    async def test_update_one_assign_by_field_name_with_no_schema(
        self, tnt: TarantoolSyncInstance
    ) -> None:
        conn = asynctnt.Connection(
            host=tnt.host,
            port=tnt.port,
            fetch_schema=False,
            reconnect_timeout=1 / 3,
        )
        await conn.connect()
        try:
            data = await self._fill_data(conn)

            res = await conn.update(TESTER_SPACE_ID, [4], [["=", "f4", 100]])
            data[4][3] = 100
            assert_response_equal(res, [data[4]], "Body ok")
        finally:
            try:
                await conn.call("truncate", timeout=5)
            except Exception:
                pass
            await conn.disconnect()

    @ensure_version(min=(2, 3))
    async def test_update_one_assign_by_json(self, conn: asynctnt.Connection) -> None:
        data = await self._fill_data(conn)

        res = await conn.update(
            TESTER_SPACE_ID, [4], [["=", "f5.tree1.tree11.key1", "value2"]]
        )
        data[4][4]["tree1"]["tree11"]["key1"] = "value2"
        assert_response_equal(res, [data[4]], "Body ok")

    async def test_update_one_insert(self, conn: asynctnt.Connection) -> None:
        data = await self._fill_data(conn)

        res = await conn.update(TESTER_SPACE_ID, [1], [["!", 2, 14]])
        data[1].insert(2, 14)
        assert_response_equal(res, [data[1]], "Body ok")

    async def test_update_one_delete(self, conn: asynctnt.Connection) -> None:
        data = await self._fill_data(conn)

        res = await conn.update(TESTER_SPACE_ID, [2], [["#", 5, 1]])
        data[2].pop(5)
        assert_response_equal(res, [data[2]], "Body ok")

    async def test_update_one_plus(self, conn: asynctnt.Connection) -> None:
        data = await self._fill_data(conn)

        res = await conn.update(TESTER_SPACE_ID, [1], [["+", 2, 3]])
        data[1][2] += 3
        assert_response_equal(res, [data[1]], "Body ok")

    @ensure_version(min=(2, 3))
    async def test_update_one_plus_decimal(self, conn: asynctnt.Connection) -> None:
        data = await self._fill_data(conn)

        add = Decimal("1.1")
        res = await conn.update(TESTER_SPACE_ID, [3], [["+", 5, add]])
        data[3][5] += add
        assert_response_equal(res, [data[3]], "Body ok")

    @ensure_version(min=(2, 3))
    async def test_update_one_plus_float(self, conn: asynctnt.Connection) -> None:
        data = await self._fill_data(conn)

        add = 1.5
        res = await conn.update(TESTER_SPACE_ID, [3], [["+", 6, add]])
        data[3][6] += add
        assert_response_equal(res, [data[3]], "Body ok")

    async def test_update_one_plus_str_field(self, conn: asynctnt.Connection) -> None:
        data = await self._fill_data(conn)

        res = await conn.update(TESTER_SPACE_ID, [1], [["+", "f3", 3]])
        data[1][2] += 3
        assert_response_equal(res, [data[1]], "Body ok")

    @ensure_version(min=(2, 3))
    async def test_update_one_plus_str_field_unknown(
        self, conn: asynctnt.Connection
    ) -> None:
        await self._fill_data(conn)

        with pytest.raises(
            TarantoolDatabaseError, match="Field 'f10' was not found in the tuple"
        ):
            await conn.update(TESTER_SPACE_ID, [1], [["+", "f10", 3]])

    async def test_update_one_plus_negative(self, conn: asynctnt.Connection) -> None:
        data = await self._fill_data(conn)

        res = await conn.update(TESTER_SPACE_ID, [1], [["+", 2, -3]])
        data[1][2] += -3
        assert_response_equal(res, [data[1]], "Body ok")

    async def test_update_one_minus(self, conn: asynctnt.Connection) -> None:
        data = await self._fill_data(conn)

        res = await conn.update(TESTER_SPACE_ID, [0], [["-", 2, 1]])
        data[0][2] -= 1
        assert_response_equal(res, [data[0]], "Body ok")

    async def test_update_one_minus_negative(self, conn: asynctnt.Connection) -> None:
        data = await self._fill_data(conn)

        res = await conn.update(TESTER_SPACE_ID, [1], [["-", 2, -3]])
        data[1][2] -= -3
        assert_response_equal(res, [data[1]], "Body ok")

    async def test_update_one_band(self, conn: asynctnt.Connection) -> None:
        data = await self._fill_data(conn)

        res = await conn.update(TESTER_SPACE_ID, [1], [["&", 2, 3]])
        data[1][2] &= 3
        assert_response_equal(res, [data[1]], "Body ok")

        res = await conn.update(TESTER_SPACE_ID, [1], [["&", 2, 2]])
        data[1][2] &= 2
        assert_response_equal(res, [data[1]], "Body ok")

        res = await conn.update(TESTER_SPACE_ID, [1], [["&", 2, 1]])
        data[1][2] &= 1
        assert_response_equal(res, [data[1]], "Body ok")

        res = await conn.update(TESTER_SPACE_ID, [1], [["&", 2, 0]])
        data[1][2] &= 0
        assert_response_equal(res, [data[1]], "Body ok")

    async def test_update_one_bor(self, conn: asynctnt.Connection) -> None:
        data = await self._fill_data(conn)

        res = await conn.update(TESTER_SPACE_ID, [1], [["|", 2, 3]])
        data[1][2] |= 3
        assert_response_equal(res, [data[1]], "Body ok")

        res = await conn.update(TESTER_SPACE_ID, [1], [["|", 2, 2]])
        data[1][2] |= 2
        assert_response_equal(res, [data[1]], "Body ok")

        res = await conn.update(TESTER_SPACE_ID, [1], [["|", 2, 1]])
        data[1][2] |= 1
        assert_response_equal(res, [data[1]], "Body ok")

        res = await conn.update(TESTER_SPACE_ID, [1], [["|", 2, 0]])
        data[1][2] |= 0
        assert_response_equal(res, [data[1]], "Body ok")

    async def test_update_one_bxor(self, conn: asynctnt.Connection) -> None:
        data = await self._fill_data(conn)

        res = await conn.update(TESTER_SPACE_ID, [1], [["^", 2, 3]])
        data[1][2] ^= 3
        assert_response_equal(res, [data[1]], "Body ok")

        res = await conn.update(TESTER_SPACE_ID, [1], [["^", 2, 2]])
        data[1][2] ^= 2
        assert_response_equal(res, [data[1]], "Body ok")

        res = await conn.update(TESTER_SPACE_ID, [1], [["^", 2, 1]])
        data[1][2] ^= 1
        assert_response_equal(res, [data[1]], "Body ok")

        res = await conn.update(TESTER_SPACE_ID, [1], [["^", 2, 0]])
        data[1][2] ^= 0
        assert_response_equal(res, [data[1]], "Body ok")

    @pytest.mark.min_bin_version((2, 3))
    async def test_update_operations_not_int_without_schema(
        self, tnt: TarantoolSyncInstance
    ) -> None:
        conn = asynctnt.Connection(
            host=tnt.host,
            port=tnt.port,
            fetch_schema=False,
            reconnect_timeout=1 / 3,
        )
        await conn.connect()
        try:
            data = [1, "hello2", 1, 4, "what is up"]
            await conn.insert(TESTER_SPACE_ID, data)

            res = await conn.update(TESTER_SPACE_ID, [1], [["+", "f3", 1]])
            data[2] += 1
            assert_response_equal(res, [data], "Body ok")
        finally:
            try:
                await conn.call("truncate", timeout=5)
            except Exception:
                pass
            await conn.disconnect()

    async def test_update_splice(self, conn: asynctnt.Connection) -> None:
        data = [1, "hello2", 1, 4, "what is up"]
        await conn.insert(TESTER_SPACE_ID, data)

        res = await conn.update(TESTER_SPACE_ID, [1], [[":", 1, 1, 3, "!!!"]])

        data[1] = "h!!!o2"
        assert_response_equal(res, [data], "Body ok")

    async def test_update_splice_bytes(self, conn: asynctnt.Connection) -> None:
        data = [1, "hello2", 1, 4, "what is up", -5]
        await conn.insert(TESTER_SPACE_ID, data)

        res = await conn.update(TESTER_SPACE_ID, [1], [[b":", 1, 1, 3, "!!!"]])

        data[1] = "h!!!o2"
        assert_response_equal(res, [data], "Body ok")

    async def test_update_splice_wrong_args(self, conn: asynctnt.Connection) -> None:
        data = [1, "hello2", 1, 4, "what is up"]
        await conn.insert(TESTER_SPACE_ID, data)

        with pytest.raises(IndexError, match=r"Operation length must be at least 3"):
            await conn.update(TESTER_SPACE_ID, [1], [[":", 2]])

        with pytest.raises(ValueError, match=r"Splice operation must have length of 5"):
            await conn.update(TESTER_SPACE_ID, [1], [[":", 2, 1]])

        with pytest.raises(ValueError, match=r"Splice operation must have length of 5"):
            await conn.update(TESTER_SPACE_ID, [1], [[":", 2, 1, 3]])

        with pytest.raises(TypeError, match=r"Splice offset must be int"):
            await conn.update(TESTER_SPACE_ID, [1], [[":", 2, 1, {}, ":::"]])

        with pytest.raises(TypeError, match=r"Splice position must be int"):
            await conn.update(TESTER_SPACE_ID, [1], [[":", 2, {}, {}, ":::"]])

        with pytest.raises(
            TypeError, match=r"Operation field_no must be of either int or str type"
        ):
            await conn.update(TESTER_SPACE_ID, [1], [[":", {}, {}, {}, ":::"]])

        with pytest.raises(TypeError, match=r"Unknown update operation type `yo`"):
            await conn.update(TESTER_SPACE_ID, [1], [["yo", 1, 2, 3]])

        with pytest.raises(
            TypeError, match=r"Operation type must of a str or bytes type"
        ):
            await conn.update(TESTER_SPACE_ID, [1], [[{}, 1, 2, 3]])

        with pytest.raises(
            TypeError, match=r"Single operation must be a tuple or list"
        ):
            await conn.update(TESTER_SPACE_ID, [1], [{}])

        with pytest.raises(TarantoolDatabaseError) as exc:
            await conn.update(TESTER_SPACE_ID, [1], [("+", 2, {})])
        assert "Argument type in operation '+' on field" in exc.value.message

    async def test_update_multiple_operations(self, conn: asynctnt.Connection) -> None:
        t = [1, "1", 1, 5, "hello", 3, 4, 8]
        await conn.insert(TESTER_SPACE_ID, t)

        t[2] += 1
        t[3] -= 4
        t[5] &= 5
        t[6] |= 7
        t[7] = 100
        t[4] = "h!!!o"

        operations = [
            ["+", 2, 1],
            ["-", 3, 4],
            ["&", 5, 5],
            ["|", 6, 7],
            ["=", 7, 100],
            [":", 4, 1, 3, "!!!"],
        ]

        res = await conn.update(TESTER_SPACE_ID, [1], operations)
        assert_response_equal(res, [t], "Body ok")

    async def test_update_by_name(self, conn: asynctnt.Connection) -> None:
        data = await self._fill_data(conn)

        res = await conn.update(TESTER_SPACE_NAME, [1], [["=", 2, 2]])

        data[1][2] = 2
        assert isinstance(res, Response), "Got response"
        assert res.code == 0, "success"
        assert res.sync > 0, "sync > 0"
        assert_response_equal(res, [data[1]], "Body ok")

    async def test_update_by_name_no_schema(self, tnt: TarantoolSyncInstance) -> None:
        conn = asynctnt.Connection(
            host=tnt.host,
            port=tnt.port,
            fetch_schema=True,
            reconnect_timeout=1 / 3,
        )
        await conn.connect()
        try:
            await self._fill_data(conn)

            await conn.disconnect()

            conn = asynctnt.Connection(
                host=tnt.host,
                port=tnt.port,
                fetch_schema=False,
                auto_refetch_schema=False,
                reconnect_timeout=1 / 3,
            )
            await conn.connect()

            with pytest.raises(TarantoolSchemaError):
                await conn.update(TESTER_SPACE_NAME, [1], [["=", 2, 2]])
        finally:
            try:
                await conn.call("truncate", timeout=5)
            except Exception:
                pass
            await conn.disconnect()

    async def test_update_by_index_id(
        self, conn: asynctnt.Connection, tnt: TarantoolSyncInstance
    ) -> None:
        index_name = "temp_idx"
        res = tnt.command(f'make_third_index("{index_name}")')
        index_id = res[0][0]

        try:
            await conn.disconnect()
            await conn.connect()
            data = await self._fill_data(conn)

            res = await conn.update(
                TESTER_SPACE_ID, [data[0][2]], [("=", 2, 1)], index=index_id
            )

            data[0][2] = 1
            assert isinstance(res, Response), "Got response"
            assert res.code == 0, "success"
            assert res.sync > 0, "sync > 0"
            assert_response_equal(res, [data[0]], "Body ok")
        finally:
            tnt.command(f"box.space.{TESTER_SPACE_NAME}.index.{index_name}:drop()")

    async def test_select_by_index_name(
        self, conn: asynctnt.Connection, tnt: TarantoolSyncInstance
    ) -> None:
        index_name = "temp_idx"
        tnt.command(f'make_third_index("{index_name}")')

        try:
            await conn.disconnect()
            await conn.connect()
            data = await self._fill_data(conn)

            res = await conn.update(
                TESTER_SPACE_ID, [data[0][2]], [["=", 2, 1]], index=index_name
            )

            data[0][2] = 1
            assert isinstance(res, Response), "Got response"
            assert res.code == 0, "success"
            assert res.sync > 0, "sync > 0"
            assert_response_equal(res, [data[0]], "Body ok")
        finally:
            tnt.command(f"box.space.{TESTER_SPACE_NAME}.index.{index_name}:drop()")

    async def test_update_by_index_id_no_schema(
        self, tnt: TarantoolSyncInstance
    ) -> None:
        conn = asynctnt.Connection(
            host=tnt.host,
            port=tnt.port,
            fetch_schema=True,
            reconnect_timeout=1 / 3,
        )
        await conn.connect()
        try:
            await self._fill_data(conn)
            await conn.disconnect()

            conn = asynctnt.Connection(
                host=tnt.host,
                port=tnt.port,
                fetch_schema=False,
                auto_refetch_schema=False,
                reconnect_timeout=1 / 3,
            )
            await conn.connect()

            # Should not raise
            await conn.update(TESTER_SPACE_ID, [0], [["=", 2, 1]], index=0)
        finally:
            try:
                await conn.call("truncate", timeout=5)
            except Exception:
                pass
            await conn.disconnect()

    async def test_update_by_index_name_no_schema(
        self, conn_no_schema: asynctnt.Connection
    ) -> None:
        with pytest.raises(TarantoolSchemaError):
            await conn_no_schema.update(
                TESTER_SPACE_NAME, [0], [["=", 2, 1]], index="primary"
            )

    async def test_update_operations_none(self, conn: asynctnt.Connection) -> None:
        data = await self._fill_data(conn)
        try:
            res = await conn.update(TESTER_SPACE_NAME, [data[0][0]], None)
        except TarantoolDatabaseError as e:
            if conn.version < (1, 7):
                if e.code == ErrorCode.ER_ILLEGAL_PARAMS:
                    # success
                    return
            raise
        assert_response_equal(res, [data[0]], "empty operations")

    async def test_update_dict_key(self, conn: asynctnt.Connection) -> None:
        data = await self._fill_data(conn)

        res = await conn.update(TESTER_SPACE_ID, {"f1": 0}, [["+", 3, 1]])
        data[0][3] += 1
        assert_response_equal(res, [data[0]], "Body ok")

    async def test_update_dict_resp(self, conn: asynctnt.Connection) -> None:
        data = await self._fill_data(conn)

        res = await conn.update(TESTER_SPACE_ID, [0], [["+", 3, 1]])
        data[0][3] += 1

        assert_response_equal_kv(
            res,
            [
                {
                    "f1": data[0][0],
                    "f2": data[0][1],
                    "f3": data[0][2],
                    "f4": data[0][3],
                    "f5": data[0][4],
                }
            ],
        )
