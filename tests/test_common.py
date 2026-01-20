"""Tests for common functionality."""

from __future__ import annotations

import asyncio

import pytest

import asynctnt
from asynctnt.exceptions import TarantoolDatabaseError, TarantoolNotConnectedError
from asynctnt.instance import TarantoolSyncInstance
from tests.conftest import TESTER_SPACE_ID, TESTER_SPACE_NAME
from tests.utils.assertions import assert_response_equal
from tests.utils.params import get_big_param, get_complex_param


class TestCommon:
    """Common functionality tests."""

    async def test_encoding_utf8(self, conn: asynctnt.Connection) -> None:
        p, p_cmp = get_complex_param(replace_bin=False)

        data = [1, "hello", 1, 0, p]
        data_cmp = [1, "hello", 1, 0, p_cmp]

        res = await conn.insert(TESTER_SPACE_ID, data)
        assert_response_equal(res, [data_cmp], "Body ok")

        res = await conn.select(TESTER_SPACE_ID)
        assert_response_equal(res, [data_cmp], "Body ok")

    async def test_encoding_cp1251(self, tnt: TarantoolSyncInstance) -> None:
        conn = asynctnt.Connection(
            host=tnt.host,
            port=tnt.port,
            encoding="cp1251",
        )
        await conn.connect()
        try:
            p, p_cmp = get_complex_param(replace_bin=False)

            data = [1, "hello", 1, 0, p]
            data_cmp = [1, "hello", 1, 0, p_cmp]

            res = await conn.insert(TESTER_SPACE_ID, data)
            assert_response_equal(res, [data_cmp], "Body ok")

            res = await conn.select(TESTER_SPACE_ID)
            assert_response_equal(res, [data_cmp], "Body ok")
        finally:
            if conn.is_connected:
                await conn.call("truncate", timeout=5)
            await conn.disconnect()

    async def test_schema_refetch_on_schema_change(
        self, tnt: TarantoolSyncInstance
    ) -> None:
        conn = asynctnt.Connection(
            host=tnt.host,
            port=tnt.port,
            auto_refetch_schema=True,
            username="t1",
            password="t1",
        )
        await conn.connect()
        try:
            assert conn.fetch_schema
            assert conn.auto_refetch_schema
            schema_before = conn.schema_id
            assert schema_before != -1

            # Changing scheme
            await conn.eval("box.schema.create_space('new_space');")

            try:
                await conn.ping()  # Should not raise

                # wait for schema to refetch
                await asyncio.sleep(1)

                assert conn.schema_id > schema_before, "Schema changed"
                assert "new_space" in conn.schema.spaces
            finally:
                await conn.eval(
                    "local s = box.space.new_space;if s ~= nil then s:drop(); end"
                )
        finally:
            if conn.is_connected:
                await conn.call("truncate", timeout=5)
            await conn.disconnect()

    async def test_schema_refetch_manual(self, tnt: TarantoolSyncInstance) -> None:
        conn = asynctnt.Connection(
            host=tnt.host,
            port=tnt.port,
            fetch_schema=True,
            auto_refetch_schema=False,
            username="t1",
            password="t1",
        )
        await conn.connect()
        try:
            assert conn.fetch_schema
            assert not conn.auto_refetch_schema
            schema_before = conn.schema_id
            assert schema_before != -1

            await conn.call("change_format")

            await conn.ping()  # Should not raise

            assert conn.schema_id == schema_before, "schema not changed"

            await conn.refetch_schema()

            assert conn.schema_id > schema_before, "Schema changed"
            sp = conn.schema.spaces[TESTER_SPACE_NAME]
            assert 6 == len(sp.metadata.fields)
            assert "f6" == sp.metadata.fields[5].name
            assert "*" == sp.metadata.fields[5].type
        finally:
            if conn.is_connected:
                await conn.call("truncate", timeout=5)
            await conn.disconnect()

    async def test_schema_no_fetch_and_refetch(
        self, tnt: TarantoolSyncInstance
    ) -> None:
        conn = asynctnt.Connection(
            host=tnt.host,
            port=tnt.port,
            auto_refetch_schema=False,
            username="t1",
            password="t1",
            fetch_schema=False,
        )
        await conn.connect()
        try:
            assert not conn.fetch_schema
            assert not conn.auto_refetch_schema
            assert conn.schema_id == -1

            # Changing scheme
            await conn.eval("s = box.schema.create_space('new_space');s:drop();")

            await conn.ping()  # Should not raise

            await asyncio.sleep(1)  # wait for potential schema refetch

            assert conn.schema_id == -1
        finally:
            if conn.is_connected:
                await conn.call("truncate", timeout=5)
            await conn.disconnect()

    async def test_parse_numeric_map_keys(self, conn: asynctnt.Connection) -> None:
        res = await conn.eval("""return {
                [1] = 1,
                [2] = 2,
                hello = 3,
                world = 4,
                [-3] = 5,
                [4.5] = 6
            }""")

        d = {1: 1, 2: 2, "hello": 3, "world": 4, -3: 5, 4.5: 6}

        assert res[0] == d, "Numeric keys parsed ok"

    async def test_read_buffer_reallocate_ok(self, tnt: TarantoolSyncInstance) -> None:
        conn = asynctnt.Connection(
            host=tnt.host,
            port=tnt.port,
            initial_read_buffer_size=1,
        )
        await conn.connect()
        try:
            p, cmp = get_complex_param(
                encoding=conn.encoding, replace_bin=conn.version < (3, 0)
            )
            res = await conn.call("func_param", [p])  # Should not raise
            assert res[0][0] == cmp, "Body ok"
        finally:
            if conn.is_connected:
                await conn.call("truncate", timeout=5)
            await conn.disconnect()

    async def test_read_buffer_deallocate_ok(self, tnt: TarantoolSyncInstance) -> None:
        size = 100 * 1000
        conn = asynctnt.Connection(
            host=tnt.host,
            port=tnt.port,
            initial_read_buffer_size=size,
        )
        await conn.connect()
        try:
            # Waiting big response, so ReadBuffer grows to hold it
            p = get_big_param(size=size * 3)
            await conn.call("func_param", [p])  # Should not raise

            # Waiting small response, so ReadBuffer deallocates memory
            p = get_big_param(size=10)
            await conn.call("func_param", [p])  # Should not raise
        finally:
            if conn.is_connected:
                await conn.call("truncate", timeout=5)
            await conn.disconnect()

    async def test_write_buffer_reallocate(self, conn: asynctnt.Connection) -> None:
        p = get_big_param(size=100 * 1024)
        res = await conn.call("func_param", [p])  # Should not raise
        assert res[0][0] == p, "Body ok"

    async def test_ensure_no_attribute_error_on_not_connected(
        self, tnt: TarantoolSyncInstance
    ) -> None:
        conn = asynctnt.Connection(host=tnt.host, port=tnt.port)

        with pytest.raises(TarantoolNotConnectedError):
            await conn.ping()

    async def test_encode_unsupported_type(self, conn: asynctnt.Connection) -> None:
        class A:
            pass

        with pytest.raises(
            TypeError, match=r"Type `(.+)` is not supported for encoding"
        ):
            await conn.call("func_param", [{"a": A()}])

    async def test_schema_refetch_next_byte(self, tnt: TarantoolSyncInstance) -> None:
        conn = asynctnt.Connection(
            host=tnt.host,
            port=tnt.port,
            auto_refetch_schema=True,
            username="t1",
            password="t1",
        )
        await conn.connect()
        try:
            await conn.call("func_hello")

            # Changing scheme
            for _ in range(251):
                await conn.eval("s = box.schema.create_space('new_space');s:drop();")

            for _ in range(1, 255):
                await conn.call("func_hello")
        except TarantoolDatabaseError:
            pytest.fail("TarantoolDatabaseError raised unexpectedly")
        finally:
            if conn.is_connected:
                await conn.call("truncate", timeout=5)
            await conn.disconnect()

    async def test_schema_refetch_unknown_space(
        self, tnt: TarantoolSyncInstance
    ) -> None:
        conn = asynctnt.Connection(
            host=tnt.host,
            port=tnt.port,
            auto_refetch_schema=True,
            username="t1",
            password="t1",
            ping_timeout=0.1,
        )
        await conn.connect()
        try:

            async def func() -> None:
                # trying to select from an unknown space until it is created
                while True:
                    try:
                        await conn.select("spacex")
                        return
                    except Exception:
                        pass

                    await asyncio.sleep(0.1)

            f = asyncio.ensure_future(asyncio.wait_for(func(), timeout=1))

            # Changing scheme
            conn2 = await asynctnt.connect(
                host=tnt.host, port=tnt.port, username="t1", password="t1"
            )
            async with conn2:
                await conn2.eval(
                    "s = box.schema.create_space('spacex');s:create_index('primary');"
                )

            try:
                await f
            except (asyncio.TimeoutError, asyncio.CancelledError) as e:
                pytest.fail(f"Schema is not updated: {type(e)} {e}")
        finally:
            if conn.is_connected:
                await conn.call("truncate", timeout=5)
            await conn.disconnect()

    async def test_schema_refetch_on_disconnect_race_condition(
        self, tnt: TarantoolSyncInstance
    ) -> None:
        conn = asynctnt.Connection(
            host=tnt.host, port=tnt.port, username="t1", password="t1"
        )
        await conn.connect()
        try:
            await conn.eval("require('msgpack').cfg{encode_use_tostring = true}")
            await conn.call("box.schema.space.create", ["geo", {"if_not_exists": True}])
            await conn.call(
                "box.space.geo:format", [[{"name": "id", "type": "string"}]]
            )
        finally:
            await conn.disconnect()
