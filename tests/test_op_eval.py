"""Tests for eval operation."""

from __future__ import annotations

import asyncio

import pytest

import asynctnt
from asynctnt import Response
from tests.utils.assertions import assert_response_equal
from tests.utils.params import get_complex_param


class TestEval:
    """Eval operation tests."""

    async def test_eval_basic(self, conn: asynctnt.Connection) -> None:
        res = await conn.eval('return "hola"')

        assert isinstance(res, Response), "Got eval response"
        assert res.code == 0, "success"
        assert res.sync > 0, "sync > 0"
        assert_response_equal(res, ["hola"], "Body ok")

    async def test_eval_basic_pack(self, conn: asynctnt.Connection) -> None:
        res = await conn.eval('return {"hola"}')

        assert isinstance(res, Response), "Got eval response"
        assert res.code == 0, "success"
        assert res.sync > 0, "sync > 0"
        assert_response_equal(res, [["hola"]], "Body ok")

    async def test_eval_with_param(self, conn: asynctnt.Connection) -> None:
        args = [1, 2, 3, "hello"]
        res = await conn.eval("return ...", args)

        assert_response_equal(res, args, "Body ok")

    async def test_eval_with_param_pack(self, conn: asynctnt.Connection) -> None:
        args = [1, 2, 3, "hello"]
        res = await conn.eval("return {...}", args)

        assert_response_equal(res, [args], "Body ok")

    async def test_eval_func_name_invalid_type(self, conn: asynctnt.Connection) -> None:
        with pytest.raises(TypeError):
            await conn.eval(12)

        with pytest.raises(TypeError):
            await conn.eval([1, 2])

        with pytest.raises(TypeError):
            await conn.eval({"a": 1})

        with pytest.raises(TypeError):
            await conn.eval(b"qwer")

    async def test_eval_params_invalid_type(self, conn: asynctnt.Connection) -> None:
        with pytest.raises(TypeError):
            await conn.eval("return {...}", 220349)

        with pytest.raises(TypeError):
            await conn.eval("return {...}", "hey")

        with pytest.raises(TypeError):
            await conn.eval("return {...}", {1: 1, 2: 2})

    async def test_eval_args_tuple(self, conn: asynctnt.Connection) -> None:
        # Should not raise
        await conn.eval("return {...}", (1, 2))

    async def test_eval_complex_param(self, conn: asynctnt.Connection) -> None:
        p, cmp = get_complex_param(
            encoding=conn.encoding, replace_bin=conn.version < (3, 0)
        )
        res = await conn.eval("return {...}", [p])
        assert res[0][0] == cmp, "Body ok"

    async def test_eval_timeout_in_time(self, conn: asynctnt.Connection) -> None:
        # Should not raise
        cmd = """
        local args = {...}
        local fiber = require("fiber")
        fiber.sleep(args[1])
        """
        await conn.eval(cmd, [0.1], timeout=1)

    async def test_eval_timeout_late(self, conn: asynctnt.Connection) -> None:
        cmd = """
        local args = {...}
        local fiber = require("fiber")
        fiber.sleep(args[1])
        """
        with pytest.raises(asyncio.TimeoutError):
            await conn.eval(cmd, [0.3], timeout=0.1)
