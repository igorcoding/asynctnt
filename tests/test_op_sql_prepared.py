from asynctnt import Response
from asynctnt._testbase import ensure_version
from asynctnt.prepared import PreparedStatement
from tests import BaseTarantoolTestCase


class SQLPreparedStatementTestCase(BaseTarantoolTestCase):
    @ensure_version(min=(2, 0))
    async def test__basic(self):
        stmt = self.conn.prepare('select 1, 2')
        self.assertIsInstance(stmt, PreparedStatement, 'Got correct instance')
        async with stmt:
            self.assertIsNotNone(stmt.id, 'statement has been prepared')

            res = await stmt.execute()
            self.assertIsInstance(res, Response, 'Got response')
            self.assertEqual(res.code, 0, 'success')
            self.assertGreater(res.sync, 0, 'sync > 0')
            self.assertResponseEqual(res, [[1, 2]], 'Body ok')

        self.assertIsNone(stmt.id, 'statement has been unprepared')

    @ensure_version(min=(2, 0))
    async def test__manual(self):
        stmt = self.conn.prepare('select 1, 2')
        self.assertIsInstance(stmt, PreparedStatement, 'Got correct instance')
        stmt_id = await stmt.prepare()
        self.assertIsNotNone(stmt_id, 'statement has been prepared')
        res = await stmt.execute()
        self.assertIsInstance(res, Response, 'Got response')
        self.assertEqual(res.code, 0, 'success')
        self.assertGreater(res.sync, 0, 'sync > 0')
        self.assertResponseEqual(res, [[1, 2]], 'Body ok')
        await stmt.unprepare()

    @ensure_version(min=(2, 0))
    async def test__manual_iproto(self):
        res = await self.conn.prepare_iproto('select 1, 2')
        self.assertEqual(res.code, 0, 'success')
        stmt_id = res.stmt_id
        self.assertNotEqual(stmt_id, 0, 'received statement_id')

        res = await self.conn.execute(stmt_id, [])
        self.assertIsInstance(res, Response, 'Got response')
        self.assertEqual(res.code, 0, 'success')
        self.assertGreater(res.sync, 0, 'sync > 0')
        self.assertResponseEqual(res, [[1, 2]], 'Body ok')

        res = await self.conn.unprepare_iproto(stmt_id)
        self.assertEqual(res.code, 0, 'success')

    @ensure_version(min=(2, 0))
    async def test__bind(self):
        stmt = self.conn.prepare('select 1, 2 where 1 = ? and 2 = ?')
        async with stmt:
            res = await stmt.execute([1, 2])
            self.assertIsInstance(res, Response, 'Got response')
            self.assertEqual(res.code, 0, 'success')
            self.assertGreater(res.sync, 0, 'sync > 0')
            self.assertResponseEqual(res, [[1, 2]], 'Body ok')

    @ensure_version(min=(2, 0))
    async def test__bind_metadata(self):
        stmt = self.conn.prepare('select 1, 2 where 1 = :a and 2 = :b')
        async with stmt:
            self.assertIsNotNone(stmt.params_count)
            self.assertIsNotNone(stmt.params)

            self.assertEqual(2, stmt.params_count)
            self.assertEqual(':a', stmt.params.fields[0].name)
            self.assertEqual('ANY', stmt.params.fields[0].type)
            self.assertEqual(':b', stmt.params.fields[1].name)
            self.assertEqual('ANY', stmt.params.fields[1].type)

    @ensure_version(min=(2, 0))
    async def test__bind_2_execute(self):
        stmt = self.conn.prepare('select 1, 2 where 1 = ? and 2 = ?')
        async with stmt:
            res = await stmt.execute([1, 2])
            self.assertIsInstance(res, Response, 'Got response')
            self.assertEqual(res.code, 0, 'success')
            self.assertGreater(res.sync, 0, 'sync > 0')
            self.assertResponseEqual(res, [[1, 2]], 'Body ok')

            res = await stmt.execute([3, 4])
            self.assertIsInstance(res, Response, 'Got response')
            self.assertEqual(res.code, 0, 'success')
            self.assertGreater(res.sync, 0, 'sync > 0')
            self.assertResponseEqual(res, [], 'Body is empty')

    @ensure_version(min=(2, 0))
    async def test__context_manager_double_enter(self):
        stmt = self.conn.prepare('select 1, 2 where 1 = ? and 2 = ?')
        async with stmt:
            async with stmt:  # does nothing
                res = await stmt.execute([1, 2])
                self.assertResponseEqual(res, [[1, 2]], 'Body ok')
