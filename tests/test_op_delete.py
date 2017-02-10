import asyncio

from asynctnt import Iterator
from asynctnt import Response
from asynctnt.exceptions import TarantoolSchemaError
from tests import BaseTarantoolTestCase
from tests.util import get_complex_param


class DeleteTestCase(BaseTarantoolTestCase):
    async def _fill_data(self):
        data = [
            [0, 'a', 1],
            [1, 'b', 0],
        ]
        for t in data:
            await self.conn.insert(self.TESTER_SPACE_ID, t)

        return data

    async def test__delete_one(self):
        data = await self._fill_data()

        res = await self.conn.delete(self.TESTER_SPACE_ID, [data[0][0]])
        self.assertIsInstance(res, Response, 'Got response')
        self.assertEqual(res.code, 0, 'success')
        self.assertGreater(res.sync, 0, 'sync > 0')
        self.assertListEqual(res.body, [data[0]], 'Body ok')

        res = await self.conn.select(self.TESTER_SPACE_ID, [0])
        self.assertListEqual(res.body, [], 'Body ok')

    async def test__delete_by_name(self):
        data = await self._fill_data()

        res = await self.conn.delete(self.TESTER_SPACE_NAME, [data[0][0]])
        self.assertIsInstance(res, Response, 'Got response')
        self.assertEqual(res.code, 0, 'success')
        self.assertGreater(res.sync, 0, 'sync > 0')
        self.assertListEqual(res.body, [data[0]], 'Body ok')

        res = await self.conn.select(self.TESTER_SPACE_ID, [0])
        self.assertListEqual(res.body, [], 'Body ok')

    async def test__delete_by_name_no_schema(self):
        await self.tnt_reconnect(fetch_schema=False)

        with self.assertRaises(TarantoolSchemaError):
            await self.conn.delete(self.TESTER_SPACE_NAME, [0])

    async def test__delete_invalid_types(self):
        with self.assertRaisesRegex(
                TypeError, "missing 2 required positional arguments: 'space' and 'key'"):
            await self.conn.delete()

        with self.assertRaisesRegex(
                TypeError, r'Expected list, got '):
            await self.conn.delete(self.TESTER_SPACE_ID, (1,))

        with self.assertRaisesRegex(
                TypeError, r'Expected list, got '):
            await self.conn.delete(self.TESTER_SPACE_ID, {})
