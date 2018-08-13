from asynctnt import Response
from asynctnt.exceptions import TarantoolSchemaError
from tests import BaseTarantoolTestCase
from tests.util import get_complex_param


class InsertTestCase(BaseTarantoolTestCase):
    async def test__insert_one(self):
        data = [1, 'hello', 1, 4, 'what is up']
        res = await self.conn.insert(self.TESTER_SPACE_ID, data)

        self.assertIsInstance(res, Response, 'Got response')
        self.assertEqual(res.code, 0, 'success')
        self.assertGreater(res.sync, 0, 'sync > 0')
        self.assertResponseEqual(res, [data], 'Body ok')

    async def test__insert_by_name(self):
        data = [1, 'hello', 1, 4, 'what is up']
        res = await self.conn.insert(self.TESTER_SPACE_NAME, data)

        self.assertIsInstance(res, Response, 'Got response')
        self.assertEqual(res.code, 0, 'success')
        self.assertGreater(res.sync, 0, 'sync > 0')
        self.assertResponseEqual(res, [data], 'Body ok')

    async def test__insert_by_name_no_schema(self):
        await self.tnt_reconnect(fetch_schema=False)

        data = [1, 'hello', 1, 4, 'what is up']
        with self.assertRaises(TarantoolSchemaError):
            await self.conn.insert(self.TESTER_SPACE_NAME, data)

    async def test__insert_complex_tuple(self):
        p, p_cmp = get_complex_param(replace_bin=False)
        data = [1, 'hello', 1, 2, p]
        data_cmp = [1, 'hello', 1, 2, p_cmp]

        res = await self.conn.insert(self.TESTER_SPACE_ID, data)
        self.assertResponseEqual(res, [data_cmp], 'Body ok')

    async def test__insert_replace(self):
        data = [1, 'hello', 1, 4, 'what is up']

        await self.conn.insert(self.TESTER_SPACE_ID, data)

        try:
            data = [1, 'hello2', 1, 4, 'what is up']
            res = await self.conn.insert(self.TESTER_SPACE_ID,
                                         t=data,
                                         replace=True)

            self.assertResponseEqual(res, [data], 'Body ok')
        except Exception as e:
            self.fail(e)

    async def test__insert_invalid_types(self):
        with self.assertRaisesRegex(
                TypeError, r'missing 2 required positional arguments: '
                           r'\'space\' and \'t\''):
            await self.conn.insert()

        with self.assertRaisesRegex(
                TypeError, r'missing 1 required positional argument: \'t\''):
            await self.conn.insert(self.TESTER_SPACE_ID)

    async def test__replace(self):
        data = [1, 'hello', 1, 4, 'what is up']
        res = await self.conn.replace(self.TESTER_SPACE_ID, data)
        self.assertResponseEqual(res, [data], 'Body ok')

        data = [1, 'hello2', 1, 5, 'what is up']
        res = await self.conn.replace(self.TESTER_SPACE_ID, data)
        self.assertResponseEqual(res, [data], 'Body ok')

    async def test__replace_invalid_types(self):
        with self.assertRaisesRegex(
                TypeError, r'missing 2 required positional arguments: '
                           r'\'space\' and \'t\''):
            await self.conn.replace()

        with self.assertRaisesRegex(
                TypeError, r'missing 1 required positional argument: \'t\''):
            await self.conn.replace(self.TESTER_SPACE_ID)

    async def test__insert_dict_key(self):
        data = {
            'f1': 1,
            'f2': 'hello',
            'f3': 5,
            'f4': 6,
            'f5': 'hello dog',
        }
        data_cmp = [1, 'hello', 5, 6, 'hello dog']
        res = await self.conn.insert(self.TESTER_SPACE_ID, data)
        self.assertResponseEqual(res, [data_cmp], 'Body ok')

    async def test__insert_dict_key_holes(self):
        data = {
            'f1': 1,
            'f2': 'hello',
            'f3': 3,
            'f4': 6,
            'f5': None,
        }
        data_cmp = [1, 'hello', 3, 6, None]
        res = await self.conn.insert(self.TESTER_SPACE_ID, data)
        self.assertResponseEqual(res, [data_cmp], 'Body ok')

    async def test__insert_no_special_empty_key(self):
        data = {
            'f1': 1,
            'f2': 'hello',
            'f3': 3,
            'f4': 6,
            'f5': None,
        }
        res = await self.conn.insert(self.TESTER_SPACE_ID, data)

        with self.assertRaises(KeyError):
            res[0]['']

    async def test__insert_dict_resp(self):
        data = [0, 'hello', 0, 5, 'wow']
        res = await self.conn.insert(self.TESTER_SPACE_ID, data)
        self.assertResponseEqualKV(res, [{
            'f1': 0,
            'f2': 'hello',
            'f3': 0,
            'f4': 5,
            'f5': 'wow'
        }])

    async def test__insert_resp_extra(self):
        data = [0, 'hello', 5, 6, 'help', 'common', 'yo']
        res = await self.conn.insert(self.TESTER_SPACE_ID, data)
        self.assertResponseEqual(res, [data])

    async def test__insert_bin_as_str(self):
        try:
            (await self.conn.call('func_load_bin_str'))[0]
        except UnicodeDecodeError as e:
            self.fail(e)
