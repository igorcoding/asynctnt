import asyncio

from asynctnt import Iterator
from asynctnt import Response
from asynctnt.exceptions import TarantoolSchemaError
from tests import BaseTarantoolTestCase
from tests.util import get_complex_param


class InsertTestCase(BaseTarantoolTestCase):
    async def test__insert_one(self):
        data = [1, 'hello']
        res = await self.conn.insert(self.TESTER_SPACE_ID, data)

        self.assertIsInstance(res, Response, 'Got response')
        self.assertEqual(res.code, 0, 'success')
        self.assertGreater(res.sync, 0, 'sync > 0')
        self.assertListEqual(res.body, [data], 'Body ok')

    async def test__insert_by_name(self):
        data = [1, 'hello']
        res = await self.conn.insert(self.TESTER_SPACE_NAME, data)

        self.assertIsInstance(res, Response, 'Got response')
        self.assertEqual(res.code, 0, 'success')
        self.assertGreater(res.sync, 0, 'sync > 0')
        self.assertListEqual(res.body, [data], 'Body ok')

    async def test__insert_by_name_no_schema(self):
        await self.tnt_reconnect(fetch_schema=False)

        data = [1, 'hello']
        with self.assertRaises(TarantoolSchemaError):
            await self.conn.insert(self.TESTER_SPACE_NAME, data)

    async def test__insert_complex_tuple(self):
        p, p_cmp = get_complex_param(replace_bin=False)
        data = [1, 'hello', p]
        data_cmp = [1, 'hello', p_cmp]

        res = await self.conn.insert(self.TESTER_SPACE_ID, data)
        self.assertListEqual(res.body, [data_cmp], 'Body ok')

    async def test__insert_replace(self):
        data = [1, 'hello']

        await self.conn.insert(self.TESTER_SPACE_ID, data)

        try:
            res = await self.conn.insert(self.TESTER_SPACE_ID, [1, 'hello2'],
                                         replace=True)

            self.assertListEqual(res.body, [[1, 'hello2']], 'Body ok')
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
        data = [1, 'hello']
        res = await self.conn.replace(self.TESTER_SPACE_ID, data)
        self.assertListEqual(res.body, [data], 'Body ok')

        data = [1, 'hello2']
        res = await self.conn.replace(self.TESTER_SPACE_ID, data)
        self.assertListEqual(res.body, [data], 'Body ok')

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
            'f2': 'hello'
        }
        data_cmp = [1, 'hello', None, None, None]
        res = await self.conn.insert(self.TESTER_SPACE_ID, data)
        self.assertListEqual(res.body, [data_cmp], 'Body ok')

    async def test__insert_dict_key_holes(self):
        data = {
            'f1': 1,
            'f2': 'hello',
            'f4': 'wow'
        }
        data_cmp = [1, 'hello', None, 'wow', None]
        res = await self.conn.insert(self.TESTER_SPACE_ID, data)
        self.assertListEqual(res.body, [data_cmp], 'Body ok')

    async def test__insert_dict_resp(self):
        data = [0, 'hello', 0, 'wow']
        res = await self.conn.insert(self.TESTER_SPACE_ID, data,
                                     tuple_as_dict=True)
        self.assertListEqual(res.body, [{
            'f1': 0,
            'f2': 'hello',
            'f3': 0,
            'f4': 'wow'
        }])

    async def test__insert_dict_resp_extra(self):
        data = [0, 'hello', 5, 'wow', 'help', 'common', 'yo']
        res = await self.conn.insert(self.TESTER_SPACE_ID, data,
                                     tuple_as_dict=True)
        self.assertListEqual(res.body, [{
            'f1': 0,
            'f2': 'hello',
            'f3': 5,
            'f4': 'wow',
            'f5': 'help',
            '': ['common', 'yo']
        }])
