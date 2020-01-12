from asynctnt import Response
from asynctnt._testbase import ensure_version
from tests import BaseTarantoolTestCase


class SQLTestCase(BaseTarantoolTestCase):
    @ensure_version(min=(2, 0))
    async def test__sql_basic(self):
        res = await self.conn.sql('select 1, 2')

        self.assertIsInstance(res, Response, 'Got response')
        self.assertEqual(res.code, 0, 'success')
        self.assertGreater(res.sync, 0, 'sync > 0')
        self.assertResponseEqual(res, [[1, 2]], 'Body ok')

    @ensure_version(min=(2, 0))
    async def test__sql_with_param(self):
        res = await self.conn.sql('select 1, 2 where 1 = ?', [1])

        self.assertResponseEqual(res, [[1, 2]], 'Body ok')

    @ensure_version(min=(2, 0))
    async def test__sql_with_param_cols(self):
        res = await self.conn.sql('select 1 as a, 2 as b where 1 = ?', [1])

        self.assertResponseEqualKV(res, [{'A': 1, 'B': 2}], 'Body ok')

    @ensure_version(min=(2, 0))
    async def test__sql_insert(self):
        res = await self.conn.sql(
            "insert into sql_space (id, name) values (1, 'one')")
        self.assertEqual(1, res.rowcount, 'rowcount ok')

    @ensure_version(min=(2, 0))
    async def test__sql_empty_autoincrement(self):
        res = await self.conn.sql(
            "insert into sql_space (id, name) values (1, 'one')")
        self.assertEqual(None, res.autoincrement_ids, 'autoincrement ok')

    @ensure_version(min=(2, 0))
    async def test__sql_insert_autoincrement(self):
        res = await self.conn.sql(
            "insert into sql_space_autoincrement (name) values ('name')")
        self.assertEqual(1, res.rowcount, 'rowcount ok')
        self.assertEqual([1], res.autoincrement_ids, 'autoincrement ok')

    @ensure_version(min=(2, 0))
    async def test__sql_insert_autoincrement_multiple(self):
        res = await self.conn.sql(
            "insert into sql_space_autoincrement_multiple (name) values ('name'), ('name2')")
        self.assertEqual(2, res.rowcount, 'rowcount ok')
        self.assertEqual([1, 2], res.autoincrement_ids, 'autoincrement ok')

    @ensure_version(min=(2, 0))
    async def test__sql_insert_multiple(self):
        res = await self.conn.sql(
            "insert into sql_space (id, name) "
            "values (1, 'one'), (2, 'two')")
        self.assertEqual(2, res.rowcount, 'rowcount ok')

    @ensure_version(min=(2, 0))
    async def test__sql_update(self):
        await self.conn.sql("insert into sql_space values (1, 'one')")

        res = await self.conn.sql(
            "update sql_space set name = 'uno' where id = 1")

        self.assertEqual(1, res.rowcount, 'rowcount ok')

    @ensure_version(min=(2, 0))
    async def test__sql_update_multiple(self):
        await self.conn.sql("insert into sql_space values (1, 'one')")
        await self.conn.sql("insert into sql_space values (2, 'two')")

        res = await self.conn.sql("update sql_space set name = 'uno'")

        self.assertEqual(2, res.rowcount, 'rowcount ok')

    @ensure_version(min=(2, 0))
    async def test__sql_delete(self):
        await self.conn.sql("insert into sql_space values (1, 'one')")
        res = await self.conn.sql("delete from sql_space where name = 'one'")
        self.assertEqual(1, res.rowcount, 'rowcount ok')

    @ensure_version(min=(2, 0))
    async def test__sql_delete_multiple(self):
        await self.conn.sql("insert into sql_space values (1, 'one')")
        await self.conn.sql("insert into sql_space values (2, 'two')")

        res = await self.conn.sql("delete from sql_space")
        # TODO: wait for https://github.com/tarantool/tarantool/issues/3816
        # self.assertEqual(2, res.rowcount, 'rowcount ok')

        res = await self.conn.sql("select * from sql_space")
        self.assertEqual(0, res.rowcount, 'rowcount is surely ok')
