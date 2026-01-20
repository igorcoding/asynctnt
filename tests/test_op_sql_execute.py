"""Tests for SQL execute operation."""

from __future__ import annotations

import asynctnt
from asynctnt import Response
from tests.conftest import ensure_version
from tests.utils.assertions import assert_response_equal, assert_response_equal_kv


class TestSQLExecute:
    """SQL execute operation tests."""

    def _compat_field_name(self, conn: asynctnt.Connection, field_name: str) -> str:
        if conn.version >= (3, 0):
            return field_name
        return field_name.upper()

    async def _compat(self, conn: asynctnt.Connection) -> None:
        if conn.version >= (2, 11):
            await conn.execute('SET SESSION "sql_seq_scan" = true;')

    @ensure_version(min=(2, 0))
    async def test_sql_basic(self, conn: asynctnt.Connection) -> None:
        res = await conn.execute("select 1, 2")

        assert isinstance(res, Response), "Got response"
        assert res.code == 0, "success"
        assert res.sync > 0, "sync > 0"
        assert_response_equal(res, [[1, 2]], "Body ok")

    @ensure_version(min=(2, 0))
    async def test_sql_with_param(self, conn: asynctnt.Connection) -> None:
        res = await conn.execute("select 1, 2 where 1 = ?", [1])

        assert_response_equal(res, [[1, 2]], "Body ok")

    @ensure_version(min=(2, 0))
    async def test_sql_with_param_cols(self, conn: asynctnt.Connection) -> None:
        res = await conn.execute("select 1 as a, 2 as b where 1 = ?", [1])

        assert_response_equal_kv(
            res,
            [
                {
                    self._compat_field_name(conn, "a"): 1,
                    self._compat_field_name(conn, "b"): 2,
                }
            ],
            "Body ok",
        )

    @ensure_version(min=(2, 0))
    async def test_sql_with_param_cols2(self, conn: asynctnt.Connection) -> None:
        res = await conn.execute("select 1 as a, 2 as b where 1 = ? and 2 = ?", [1, 2])

        assert_response_equal_kv(
            res,
            [
                {
                    self._compat_field_name(conn, "a"): 1,
                    self._compat_field_name(conn, "b"): 2,
                }
            ],
            "Body ok",
        )

    @ensure_version(min=(2, 0))
    async def test_sql_with_param_cols_maps(self, conn: asynctnt.Connection) -> None:
        res = await conn.execute(
            "select 1 as a, 2 as b where 1 = :p1 and 2 = :p2",
            [
                {":p1": 1},
                {":p2": 2},
            ],
        )

        assert_response_equal_kv(
            res,
            [
                {
                    self._compat_field_name(conn, "a"): 1,
                    self._compat_field_name(conn, "b"): 2,
                }
            ],
            "Body ok",
        )

    @ensure_version(min=(2, 0))
    async def test_sql_with_param_cols_maps_and_positional(
        self, conn: asynctnt.Connection
    ) -> None:
        res = await conn.execute(
            "select 1 as a, 2 as b where 1 = :p1 and 2 = :p2 and 3 = ? and 4 = ?",
            [{":p1": 1}, {":p2": 2}, 3, 4],
        )

        assert_response_equal_kv(
            res,
            [
                {
                    self._compat_field_name(conn, "a"): 1,
                    self._compat_field_name(conn, "b"): 2,
                }
            ],
            "Body ok",
        )

    @ensure_version(min=(2, 0))
    async def test_sql_insert(self, conn: asynctnt.Connection) -> None:
        res = await conn.execute("insert into sql_space (id, name) values (1, 'one')")
        assert res.rowcount == 1, "rowcount ok"

    @ensure_version(min=(2, 0))
    async def test_sql_empty_autoincrement(self, conn: asynctnt.Connection) -> None:
        res = await conn.execute("insert into sql_space (id, name) values (1, 'one')")
        assert res.autoincrement_ids is None, "autoincrement ok"

    @ensure_version(min=(2, 0))
    async def test_sql_insert_autoincrement(self, conn: asynctnt.Connection) -> None:
        res = await conn.execute(
            "insert into sql_space_autoincrement (name) values ('name')"
        )
        assert res.rowcount == 1, "rowcount ok"
        assert res.autoincrement_ids == [1], "autoincrement ok"

    @ensure_version(min=(2, 0))
    async def test_sql_insert_autoincrement_multiple(
        self, conn: asynctnt.Connection
    ) -> None:
        res = await conn.execute(
            "insert into sql_space_autoincrement_multiple (name) "
            "values ('name'), ('name2')"
        )
        assert res.rowcount == 2, "rowcount ok"
        assert res.autoincrement_ids == [1, 2], "autoincrement ok"

    @ensure_version(min=(2, 0))
    async def test_sql_insert_multiple(self, conn: asynctnt.Connection) -> None:
        res = await conn.execute(
            "insert into sql_space (id, name) values (1, 'one'), (2, 'two')"
        )
        assert res.rowcount == 2, "rowcount ok"

    @ensure_version(min=(2, 0))
    async def test_sql_update(self, conn: asynctnt.Connection) -> None:
        await conn.execute("insert into sql_space values (1, 'one')")

        res = await conn.execute("update sql_space set name = 'uno' where id = 1")

        assert res.rowcount == 1, "rowcount ok"

    @ensure_version(min=(2, 0))
    async def test_sql_update_multiple(self, conn: asynctnt.Connection) -> None:
        await conn.execute("insert into sql_space values (1, 'one')")
        await conn.execute("insert into sql_space values (2, 'two')")

        res = await conn.execute("update sql_space set name = 'uno'")

        assert res.rowcount == 2, "rowcount ok"

    @ensure_version(min=(2, 0))
    async def test_sql_delete(self, conn: asynctnt.Connection) -> None:
        await self._compat(conn)

        await conn.execute("insert into sql_space values (1, 'one')")
        res = await conn.execute("delete from sql_space where name = 'one'")
        assert res.rowcount == 1, "rowcount ok"

    @ensure_version(min=(2, 0))
    async def test_sql_select(self, conn: asynctnt.Connection) -> None:
        await self._compat(conn)

        await conn.execute("insert into sql_space values (1, 'one')")
        await conn.execute("insert into sql_space values (2, 'two')")

        res = await conn.execute("select * from sql_space")
        assert res.rowcount == 2, "rowcount is surely ok"
        assert res.body[0][self._compat_field_name(conn, "id")] == 1
        assert res.body[0][self._compat_field_name(conn, "name")] == "one"
        assert res.body[1][self._compat_field_name(conn, "id")] == 2
        assert res.body[1][self._compat_field_name(conn, "name")] == "two"

    @ensure_version(min=(2, 0))
    async def test_sql_delete_multiple(self, conn: asynctnt.Connection) -> None:
        await self._compat(conn)

        await conn.execute("insert into sql_space values (1, 'one')")
        await conn.execute("insert into sql_space values (2, 'two')")

        res = await conn.execute("delete from sql_space")
        assert res.rowcount == 2, "rowcount ok"

        res = await conn.execute("select * from sql_space")
        assert res.rowcount == 0, "rowcount is surely ok"

    @ensure_version(min=(2, 0))
    async def test_metadata(self, conn: asynctnt.Connection) -> None:
        res = await conn.execute("select 1, 2")
        assert res.metadata is not None
        assert res.metadata.fields is not None
        assert len(res.metadata.fields) == 2

        assert res.metadata.fields[0].name == "COLUMN_1"
        assert res.metadata.fields[0].type == "integer"
        assert res.metadata.fields[1].name == "COLUMN_2"
        assert res.metadata.fields[1].type == "integer"

    @ensure_version(min=(2, 0))
    async def test_metadata_names(self, conn: asynctnt.Connection) -> None:
        await self._compat(conn)

        res = await conn.execute("select 1 as a, 2 as b")
        assert res.metadata is not None
        assert res.metadata.fields is not None
        assert len(res.metadata.fields) == 2

        assert res.metadata.fields[0].name == self._compat_field_name(conn, "a")
        assert res.metadata.fields[0].type == "integer"
        assert res.metadata.fields[1].name == self._compat_field_name(conn, "b")
        assert res.metadata.fields[1].type == "integer"

    @ensure_version(min=(2, 0))
    async def test_metadata_actual_space(self, conn: asynctnt.Connection) -> None:
        await self._compat(conn)

        await conn.execute("insert into sql_space values (1, 'one')")
        await conn.execute("insert into sql_space values (2, 'two')")

        res = await conn.execute("select * from sql_space")
        assert res.rowcount == 2, "rowcount is ok"
        assert len(res.metadata.fields) == 2
        assert res.metadata.fields[0].name == self._compat_field_name(conn, "id")
        assert res.metadata.fields[0].type == "integer"
        assert res.metadata.fields[0].is_nullable is None
        assert res.metadata.fields[0].is_autoincrement is None
        assert res.metadata.fields[0].collation is None

        assert res.metadata.fields[1].name == self._compat_field_name(conn, "name")
        assert res.metadata.fields[1].type == "string"
        assert res.metadata.fields[1].is_nullable is None
        assert res.metadata.fields[1].is_autoincrement is None
        assert res.metadata.fields[1].collation is None

    @ensure_version(min=(2, 0))
    async def test_sql_select_full_metadata(self, conn: asynctnt.Connection) -> None:
        await self._compat(conn)

        await conn.execute("insert into sql_space values (1, 'one')")
        await conn.execute("insert into sql_space values (2, 'two')")

        await conn.update(
            "_session_settings", ["sql_full_metadata"], [("=", "value", True)]
        )

        try:
            res = await conn.execute("select * from sql_space")
            assert len(res.metadata.fields) == 2
            assert res.metadata.fields[0].name == self._compat_field_name(conn, "id")
            assert res.metadata.fields[0].type == "integer"
            assert res.metadata.fields[0].is_nullable is False
            assert res.metadata.fields[1].is_autoincrement is None
            assert res.metadata.fields[0].collation is None

            assert res.metadata.fields[1].name == self._compat_field_name(conn, "name")
            assert res.metadata.fields[1].type == "string"
            assert res.metadata.fields[1].is_nullable is True
            assert res.metadata.fields[1].is_autoincrement is None
            assert res.metadata.fields[1].collation == "unicode"
        finally:
            await conn.update(
                "_session_settings", ["sql_full_metadata"], [("=", "value", False)]
            )
