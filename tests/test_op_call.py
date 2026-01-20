"""Tests for call and call16 operations."""

from __future__ import annotations

import asyncio

import pytest

import asynctnt
from asynctnt import Response
from asynctnt.exceptions import ErrorCode, TarantoolDatabaseError
from tests.utils.assertions import assert_response_equal
from tests.utils.params import get_complex_param


class TestCall:
    """Call operation tests."""

    async def test_call_basic(self, conn: asynctnt.Connection) -> None:
        res = await conn.call("func_hello")

        assert isinstance(res, Response), "Got call response"
        assert res.code == 0, "success"
        assert res.sync > 0, "sync > 0"
        assert_response_equal(res, [["hello"]], "Body ok")

    async def test_call_basic_bare(self, conn: asynctnt.Connection) -> None:
        res = await conn.call("func_hello_bare")
        cmp = ["hello"]

        assert isinstance(res, Response), "Got call response"
        assert res.code == 0, "success"
        assert res.sync > 0, "sync > 0"
        if conn.version < (1, 7):
            cmp = [cmp]
        assert_response_equal(res, cmp, "Body ok")

    async def test_call_unknown_function(self, conn: asynctnt.Connection) -> None:
        with pytest.raises(TarantoolDatabaseError) as ctx:
            await conn.call("blablabla")
        assert ctx.value.code == ErrorCode.ER_NO_SUCH_PROC

    async def test_call_with_param(self, conn: asynctnt.Connection) -> None:
        res = await conn.call("func_param", ["myparam"])

        assert isinstance(res, Response), "Got call response"
        assert_response_equal(res, [["myparam"]], "Body ok")

    async def test_call_with_param_bare(self, conn: asynctnt.Connection) -> None:
        res = await conn.call("func_param_bare", ["myparam"])
        cmp = ["myparam"]
        if conn.version < (1, 7):
            cmp = [cmp]

        assert isinstance(res, Response), "Got call response"
        assert_response_equal(res, cmp, "Body ok")

    async def test_call_func_name_invalid_type(self, conn: asynctnt.Connection) -> None:
        with pytest.raises(TypeError):
            await conn.call(12)

        with pytest.raises(TypeError):
            await conn.call([1, 2])

        with pytest.raises(TypeError):
            await conn.call({"a": 1})

        with pytest.raises(TypeError):
            await conn.call(b"qwer")

    async def test_call_params_invalid_type(self, conn: asynctnt.Connection) -> None:
        with pytest.raises(TypeError):
            await conn.call("func_param", 220349)

        with pytest.raises(TypeError):
            await conn.call("func_param", "hey")

        with pytest.raises(TypeError):
            await conn.call("func_param", {1: 1, 2: 2})

    async def test_call_args_tuple(self, conn: asynctnt.Connection) -> None:
        # Should not raise
        await conn.call("func_param", (1, 2))

    async def test_call_complex_param(self, conn: asynctnt.Connection) -> None:
        p, cmp = get_complex_param(
            encoding=conn.encoding, replace_bin=conn.version < (3, 0)
        )
        res = await conn.call("func_param", [p])
        assert res[0][0] == cmp, "Body ok"

    async def test_call_complex_param_bare(self, conn: asynctnt.Connection) -> None:
        p, cmp = get_complex_param(
            encoding=conn.encoding, replace_bin=conn.version < (3, 0)
        )
        cmp = [cmp]
        res = await conn.call("func_param_bare", [p])
        if conn.version < (1, 7):
            cmp = [cmp]
        assert_response_equal(res, cmp, "Body ok")

    async def test_call_timeout_in_time(self, conn: asynctnt.Connection) -> None:
        # Should not raise
        await conn.call("func_long", [0.1], timeout=1)

    async def test_call_timeout_late(self, conn: asynctnt.Connection) -> None:
        with pytest.raises(asyncio.TimeoutError):
            await conn.call("func_long", [0.3], timeout=0.1)

    async def test_call_raise(self, conn: asynctnt.Connection) -> None:
        with pytest.raises(TarantoolDatabaseError) as e:
            await conn.call("raise")

        assert e.value.code == 0, "code by box.error{} is 0"
        assert e.value.message == "my reason", "Reason ok"


class TestCall16:
    """Call16 operation tests."""

    async def test_call16_basic(self, conn: asynctnt.Connection) -> None:
        res = await conn.call16("func_hello")

        assert isinstance(res, Response), "Got call response"
        assert res.code == 0, "success"
        assert res.sync > 0, "sync > 0"
        assert_response_equal(res, [["hello"]], "Body ok")

    async def test_call16_basic_bare(self, conn: asynctnt.Connection) -> None:
        # Tarantool automatically wraps return result into tuple
        res = await conn.call16("func_hello")

        assert isinstance(res, Response), "Got call response"
        assert res.code == 0, "success"
        assert res.sync > 0, "sync > 0"
        assert_response_equal(res, [["hello"]], "Body ok")

    async def test_call16_unknown_function(self, conn: asynctnt.Connection) -> None:
        with pytest.raises(TarantoolDatabaseError) as ctx:
            await conn.call16("blablabla")
        assert ctx.value.code == ErrorCode.ER_NO_SUCH_PROC

    async def test_call16_with_param(self, conn: asynctnt.Connection) -> None:
        res = await conn.call16("func_param", ["myparam"])

        assert isinstance(res, Response), "Got call response"
        assert_response_equal(res, [["myparam"]], "Body ok")

    async def test_call16_with_param_bare(self, conn: asynctnt.Connection) -> None:
        # Tarantool automatically wraps return result into tuple
        res = await conn.call16("func_param_bare", ["myparam"])

        assert isinstance(res, Response), "Got call response"
        assert_response_equal(res, [["myparam"]], "Body ok")

    async def test_call16_func_name_invalid_type(
        self, conn: asynctnt.Connection
    ) -> None:
        with pytest.raises(TypeError):
            await conn.call16(12)

        with pytest.raises(TypeError):
            await conn.call16([1, 2])

        with pytest.raises(TypeError):
            await conn.call16({"a": 1})

        with pytest.raises(TypeError):
            await conn.call16(b"qwer")

    async def test_call16_params_invalid_type(self, conn: asynctnt.Connection) -> None:
        with pytest.raises(TypeError):
            await conn.call16("func_param", 220349)

        with pytest.raises(TypeError):
            await conn.call16("func_param", "hey")

        with pytest.raises(TypeError):
            await conn.call16("func_param", {1: 1, 2: 2})

    async def test_call16_args_tuple(self, conn: asynctnt.Connection) -> None:
        # Should not raise
        await conn.call16("func_param", (1, 2))

    async def test_call16_complex_param(self, conn: asynctnt.Connection) -> None:
        p, cmp = get_complex_param(
            encoding=conn.encoding, replace_bin=conn.version < (3, 0)
        )
        res = await conn.call("func_param", [p])
        assert res[0][0] == cmp, "Body ok"

    async def test_call16_complex_param_bare(self, conn: asynctnt.Connection) -> None:
        p, cmp = get_complex_param(
            encoding=conn.encoding, replace_bin=conn.version < (3, 0)
        )
        res = await conn.call16("func_param_bare", [p])
        assert res[0][0] == cmp, "Body ok"

    async def test_call16_timeout_in_time(self, conn: asynctnt.Connection) -> None:
        # Should not raise
        await conn.call16("func_long", [0.1], timeout=1)

    async def test_call_timeout_late(self, conn: asynctnt.Connection) -> None:
        with pytest.raises(asyncio.TimeoutError):
            await conn.call16("func_long", [0.3], timeout=0.1)
