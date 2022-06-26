from asynctnt import Response
from asynctnt._testbase import ensure_version
from tests import BaseTarantoolTestCase


class SQLExecuteTestCase(BaseTarantoolTestCase):
    @ensure_version(min=(2, 0))
    async def test__sql_basic(self):
        res = await self.conn.execute('select 1, 2')

        self.assertIsInstance(res, Response, 'Got response')
        self.assertEqual(res.code, 0, 'success')
        self.assertGreater(res.sync, 0, 'sync > 0')
        self.assertResponseEqual(res, [[1, 2]], 'Body ok')

    @ensure_version(min=(2, 0))
    async def test__sql_with_param(self):
        res = await self.conn.execute('select 1, 2 where 1 = ?', [1])

        self.assertResponseEqual(res, [[1, 2]], 'Body ok')

    @ensure_version(min=(2, 0))
    async def test__sql_with_param_cols(self):
        res = await self.conn.execute('select 1 as a, 2 as b where 1 = ?', [1])

        self.assertResponseEqualKV(res, [{'A': 1, 'B': 2}], 'Body ok')

    @ensure_version(min=(2, 0))
    async def test__sql_with_param_cols2(self):
        res = await self.conn.execute(
            'select 1 as a, 2 as b where 1 = ? and 2 = ?', [1, 2])

        self.assertResponseEqualKV(res, [{'A': 1, 'B': 2}], 'Body ok')

    @ensure_version(min=(2, 0))
    async def test__sql_with_param_cols_maps(self):
        res = await self.conn.execute(
            'select 1 as a, 2 as b where 1 = :p1 and 2 = :p2', [
                {':p1': 1},
                {':p2': 2},
            ])

        self.assertResponseEqualKV(res, [{'A': 1, 'B': 2}], 'Body ok')

    @ensure_version(min=(2, 0))
    async def test__sql_with_param_cols_maps_and_positional(self):
        res = await self.conn.execute(
            'select 1 as a, 2 as b '
            'where 1 = :p1 and 2 = :p2 and 3 = ? and 4 = ?', [
                {':p1': 1},
                {':p2': 2},
                3,
                4
            ])

        self.assertResponseEqualKV(res, [{'A': 1, 'B': 2}], 'Body ok')

    @ensure_version(min=(2, 0))
    async def test__sql_insert(self):
        res = await self.conn.execute(
            "insert into sql_space (id, name) values (1, 'one')")
        self.assertEqual(1, res.rowcount, 'rowcount ok')

    @ensure_version(min=(2, 0))
    async def test__sql_empty_autoincrement(self):
        res = await self.conn.execute(
            "insert into sql_space (id, name) values (1, 'one')")
        self.assertEqual(None, res.autoincrement_ids, 'autoincrement ok')

    @ensure_version(min=(2, 0))
    async def test__sql_insert_autoincrement(self):
        res = await self.conn.execute(
            "insert into sql_space_autoincrement (name) values ('name')")
        self.assertEqual(1, res.rowcount, 'rowcount ok')
        self.assertEqual([1], res.autoincrement_ids, 'autoincrement ok')

    @ensure_version(min=(2, 0))
    async def test__sql_insert_autoincrement_multiple(self):
        res = await self.conn.execute(
            "insert into sql_space_autoincrement_multiple (name) "
            "values ('name'), ('name2')"
        )
        self.assertEqual(2, res.rowcount, 'rowcount ok')
        self.assertEqual([1, 2], res.autoincrement_ids, 'autoincrement ok')

    @ensure_version(min=(2, 0))
    async def test__sql_insert_multiple(self):
        res = await self.conn.execute(
            "insert into sql_space (id, name) "
            "values (1, 'one'), (2, 'two')")
        self.assertEqual(2, res.rowcount, 'rowcount ok')

    @ensure_version(min=(2, 0))
    async def test__sql_update(self):
        await self.conn.execute("insert into sql_space values (1, 'one')")

        res = await self.conn.execute(
            "update sql_space set name = 'uno' where id = 1")

        self.assertEqual(1, res.rowcount, 'rowcount ok')

    @ensure_version(min=(2, 0))
    async def test__sql_update_multiple(self):
        await self.conn.execute("insert into sql_space values (1, 'one')")
        await self.conn.execute("insert into sql_space values (2, 'two')")

        res = await self.conn.execute("update sql_space set name = 'uno'")

        self.assertEqual(2, res.rowcount, 'rowcount ok')

    @ensure_version(min=(2, 0))
    async def test__sql_delete(self):
        await self.conn.execute("insert into sql_space values (1, 'one')")
        res = await self.conn.execute(
            "delete from sql_space where name = 'one'"
        )
        self.assertEqual(1, res.rowcount, 'rowcount ok')

    @ensure_version(min=(2, 0))
    async def test__sql_select(self):
        await self.conn.execute("insert into sql_space values (1, 'one')")
        await self.conn.execute("insert into sql_space values (2, 'two')")

        res = await self.conn.execute("select * from sql_space")
        self.assertEqual(2, res.rowcount, 'rowcount is surely ok')
        self.assertEqual(1, res.body[0]['ID'])
        self.assertEqual('one', res.body[0]['NAME'])
        self.assertEqual(2, res.body[1]['ID'])
        self.assertEqual('two', res.body[1]['NAME'])

    @ensure_version(min=(2, 0))
    async def test__sql_delete_multiple(self):
        await self.conn.execute("insert into sql_space values (1, 'one')")
        await self.conn.execute("insert into sql_space values (2, 'two')")

        res = await self.conn.execute("delete from sql_space")
        self.assertEqual(2, res.rowcount, 'rowcount ok')

        res = await self.conn.execute("select * from sql_space")
        self.assertEqual(0, res.rowcount, 'rowcount is surely ok')

    @ensure_version(min=(2, 0))
    async def test__metadata(self):
        res = await self.conn.execute('select 1, 2')
        self.assertIsNotNone(res.metadata)
        self.assertIsNotNone(res.metadata.fields)
        self.assertEqual(2, len(res.metadata.fields))

        self.assertEqual('COLUMN_1', res.metadata.fields[0].name)
        self.assertEqual('integer', res.metadata.fields[0].type)
        self.assertEqual('COLUMN_2', res.metadata.fields[1].name)
        self.assertEqual('integer', res.metadata.fields[1].type)

    @ensure_version(min=(2, 0))
    async def test__metadata_names(self):
        res = await self.conn.execute('select 1 as a, 2 as b')
        self.assertIsNotNone(res.metadata)
        self.assertIsNotNone(res.metadata.fields)
        self.assertEqual(2, len(res.metadata.fields))

        self.assertEqual('A', res.metadata.fields[0].name)
        self.assertEqual('integer', res.metadata.fields[0].type)
        self.assertEqual('B', res.metadata.fields[1].name)
        self.assertEqual('integer', res.metadata.fields[1].type)

    @ensure_version(min=(2, 0))
    async def test__metadata_actual_space(self):
        await self.conn.execute("insert into sql_space values (1, 'one')")
        await self.conn.execute("insert into sql_space values (2, 'two')")

        res = await self.conn.execute("select * from sql_space")
        self.assertEqual(2, res.rowcount, 'rowcount is ok')
        self.assertEqual(2, len(res.metadata.fields))
        self.assertEqual('ID', res.metadata.fields[0].name)
        self.assertEqual('integer', res.metadata.fields[0].type)
        self.assertIsNone(res.metadata.fields[0].is_nullable)
        self.assertIsNone(res.metadata.fields[0].is_autoincrement)
        self.assertIsNone(res.metadata.fields[0].collation)

        self.assertEqual('NAME', res.metadata.fields[1].name)
        self.assertEqual('string', res.metadata.fields[1].type)
        self.assertIsNone(res.metadata.fields[1].is_nullable)
        self.assertIsNone(res.metadata.fields[1].is_autoincrement)
        self.assertIsNone(res.metadata.fields[1].collation)

    @ensure_version(min=(2, 0))
    async def test__sql_select_full_metadata(self):
        await self.conn.execute("insert into sql_space values (1, 'one')")
        await self.conn.execute("insert into sql_space values (2, 'two')")

        await self.conn.update(
            '_session_settings',
            ['sql_full_metadata'],
            [('=', 'value', True)]
        )

        try:
            res = await self.conn.execute("select * from sql_space")
            self.assertEqual(2, len(res.metadata.fields))
            self.assertEqual('ID', res.metadata.fields[0].name)
            self.assertEqual('integer', res.metadata.fields[0].type)
            self.assertEqual(False, res.metadata.fields[0].is_nullable)
            self.assertEqual(None, res.metadata.fields[1].is_autoincrement)
            self.assertIsNone(res.metadata.fields[0].collation)

            self.assertEqual('NAME', res.metadata.fields[1].name)
            self.assertEqual('string', res.metadata.fields[1].type)
            self.assertEqual(True, res.metadata.fields[1].is_nullable)
            self.assertEqual(None, res.metadata.fields[1].is_autoincrement)
            self.assertEqual('unicode', res.metadata.fields[1].collation)
        finally:
            await self.conn.update(
                '_session_settings',
                ['sql_full_metadata'],
                [('=', 'value', False)]
            )
