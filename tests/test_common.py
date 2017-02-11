from tests import BaseTarantoolTestCase
from tests.util import get_complex_param


class CommonTestCase(BaseTarantoolTestCase):
    async def test__encoding_utf8(self):
        p, p_cmp = get_complex_param(replace_bin=False)

        data = [1, 'hello', p]
        data_cmp = [1, 'hello', p_cmp]

        res = await self.conn.insert(self.TESTER_SPACE_ID, data)
        self.assertListEqual(res.body, [data_cmp], 'Body ok')

        res = await self.conn.select(self.TESTER_SPACE_ID)
        self.assertListEqual(res.body, [data_cmp], 'Body ok')

    async def test__encoding_cp1251(self):
        await self.tnt_reconnect(encoding='cp1251')
        p, p_cmp = get_complex_param(replace_bin=False)

        data = [1, 'hello', p]
        data_cmp = [1, 'hello', p_cmp]

        res = await self.conn.insert(self.TESTER_SPACE_ID, data)
        self.assertListEqual(res.body, [data_cmp], 'Body ok')

        res = await self.conn.select(self.TESTER_SPACE_ID)
        self.assertListEqual(res.body, [data_cmp], 'Body ok')

    async def test__schema_refetch_on_schema_change(self):
        await self.tnt_reconnect(auto_refetch_schema=True)
        self.assertTrue(self.conn.fetch_schema)
        self.assertTrue(self.conn.auto_refetch_schema)
        schema_before = self.conn.schema_id
        self.assertNotEqual(schema_before, -1)

        # Changing scheme
        await self.conn.eval(
            "s = box.schema.create_space('new_space');"
            "s:drop();"
        )

        try:
            await self.conn.ping()
        except Exception as e:
            self.fail(e)

        self.assertGreater(self.conn.schema_id, schema_before,
                           'Schema changed')

    async def test__schema_no_fetch_and_refetch(self):
        await self.tnt_reconnect(auto_refetch_schema=False,
                                 fetch_schema=False)
        self.assertFalse(self.conn.fetch_schema)
        self.assertFalse(self.conn.auto_refetch_schema)
        self.assertEqual(self.conn.schema_id, -1)

        # Changing scheme
        await self.conn.eval(
            "s = box.schema.create_space('new_space');"
            "s:drop();"
        )

        try:
            await self.conn.ping()
        except Exception as e:
            self.fail(e)
