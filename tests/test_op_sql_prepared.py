"""Tests for SQL prepared statement operations."""

from __future__ import annotations

import asynctnt
from asynctnt import Response
from asynctnt.prepared import PreparedStatement
from tests.conftest import ensure_version
from tests.utils.assertions import assert_response_equal


class TestSQLPreparedStatement:
    """SQL prepared statement operation tests."""

    @ensure_version(min=(2, 0))
    async def test_basic(self, conn: asynctnt.Connection) -> None:
        stmt = conn.prepare("select 1, 2")
        assert isinstance(stmt, PreparedStatement), "Got correct instance"
        async with stmt:
            assert stmt.id is not None, "statement has been prepared"

            res = await stmt.execute()
            assert isinstance(res, Response), "Got response"
            assert res.code == 0, "success"
            assert res.sync > 0, "sync > 0"
            assert_response_equal(res, [[1, 2]], "Body ok")

        assert stmt.id is None, "statement has been unprepared"

    @ensure_version(min=(2, 0))
    async def test_manual(self, conn: asynctnt.Connection) -> None:
        stmt = conn.prepare("select 1, 2")
        assert isinstance(stmt, PreparedStatement), "Got correct instance"
        stmt_id = await stmt.prepare()
        assert stmt_id is not None, "statement has been prepared"
        res = await stmt.execute()
        assert isinstance(res, Response), "Got response"
        assert res.code == 0, "success"
        assert res.sync > 0, "sync > 0"
        assert_response_equal(res, [[1, 2]], "Body ok")
        await stmt.unprepare()

    @ensure_version(min=(2, 0))
    async def test_manual_iproto(self, conn: asynctnt.Connection) -> None:
        res = await conn.prepare_iproto("select 1, 2")
        assert res.code == 0, "success"
        stmt_id = res.stmt_id
        assert stmt_id != 0, "received statement_id"

        res = await conn.execute(stmt_id, [])
        assert isinstance(res, Response), "Got response"
        assert res.code == 0, "success"
        assert res.sync > 0, "sync > 0"
        assert_response_equal(res, [[1, 2]], "Body ok")

        res = await conn.unprepare_iproto(stmt_id)
        assert res.code == 0, "success"

    @ensure_version(min=(2, 0))
    async def test_bind(self, conn: asynctnt.Connection) -> None:
        stmt = conn.prepare("select 1, 2 where 1 = ? and 2 = ?")
        async with stmt:
            res = await stmt.execute([1, 2])
            assert isinstance(res, Response), "Got response"
            assert res.code == 0, "success"
            assert res.sync > 0, "sync > 0"
            assert_response_equal(res, [[1, 2]], "Body ok")

    @ensure_version(min=(2, 0))
    async def test_bind_metadata(self, conn: asynctnt.Connection) -> None:
        stmt = conn.prepare("select 1, 2 where 1 = :a and 2 = :b")
        async with stmt:
            assert stmt.params_count is not None
            assert stmt.params is not None

            assert stmt.params_count == 2
            assert stmt.params.fields[0].name == ":a"
            assert stmt.params.fields[0].type == "ANY"
            assert stmt.params.fields[1].name == ":b"
            assert stmt.params.fields[1].type == "ANY"

    @ensure_version(min=(2, 0))
    async def test_bind_2_execute(self, conn: asynctnt.Connection) -> None:
        stmt = conn.prepare("select 1, 2 where 1 = ? and 2 = ?")
        async with stmt:
            res = await stmt.execute([1, 2])
            assert isinstance(res, Response), "Got response"
            assert res.code == 0, "success"
            assert res.sync > 0, "sync > 0"
            assert_response_equal(res, [[1, 2]], "Body ok")

            res = await stmt.execute([3, 4])
            assert isinstance(res, Response), "Got response"
            assert res.code == 0, "success"
            assert res.sync > 0, "sync > 0"
            assert_response_equal(res, [], "Body is empty")

    @ensure_version(min=(2, 0))
    async def test_context_manager_double_enter(
        self, conn: asynctnt.Connection
    ) -> None:
        stmt = conn.prepare("select 1, 2 where 1 = ? and 2 = ?")
        async with stmt:
            async with stmt:  # does nothing
                res = await stmt.execute([1, 2])
                assert_response_equal(res, [[1, 2]], "Body ok")
