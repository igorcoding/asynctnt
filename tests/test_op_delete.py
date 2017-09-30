import asyncio

from asynctnt import Iterator
from asynctnt import Response
from asynctnt.exceptions import TarantoolSchemaError
from tests import BaseTarantoolTestCase
from tests.util import get_complex_param


class DeleteTestCase(BaseTarantoolTestCase):
    async def _fill_data(self):
        data = [
            [0, 'a', 1, 2, 'hello my darling'],
            [1, 'b', 3, 4, 'hello my darling, again'],
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

    async def test__delete_by_index_id(self):
        index_name = 'temp_idx'
        res = self.tnt.command(
            'make_third_index("{}")'.format(index_name)
        )
        index_id = res[0][0]

        try:
            await self.tnt_reconnect()

            data = await self._fill_data()

            res = await self.conn.delete(self.TESTER_SPACE_NAME, [data[1][2]],
                                         index=index_id)
            self.assertIsInstance(res, Response, 'Got response')
            self.assertEqual(res.code, 0, 'success')
            self.assertGreater(res.sync, 0, 'sync > 0')
            self.assertListEqual(res.body, [data[1]], 'Body ok')

            res = await self.conn.select(self.TESTER_SPACE_ID, [data[1][2]],
                                         index=index_id)
            self.assertListEqual(res.body, [], 'Body ok')
        finally:
            self.tnt.command(
                'box.space.{}.index.{}:drop()'.format(
                    self.TESTER_SPACE_NAME, index_name)
            )

    async def test__delete_by_index_name(self):
        index_name = 'temp_idx'
        res = self.tnt.command(
            'make_third_index("{}")'.format(index_name)
        )
        index_id = res[0][0]

        try:
            await self.tnt_reconnect()

            data = await self._fill_data()

            res = await self.conn.delete(self.TESTER_SPACE_NAME, [data[1][2]],
                                         index=index_name)
            self.assertIsInstance(res, Response, 'Got response')
            self.assertEqual(res.code, 0, 'success')
            self.assertGreater(res.sync, 0, 'sync > 0')
            self.assertListEqual(res.body, [data[1]], 'Body ok')

            res = await self.conn.select(self.TESTER_SPACE_ID, [data[1][2]],
                                         index=index_id)
            self.assertListEqual(res.body, [], 'Body ok')
        finally:
            self.tnt.command(
                'box.space.{}.index.{}:drop()'.format(
                    self.TESTER_SPACE_NAME, index_name)
            )

    async def test__delete_by_name_no_schema(self):
        await self.tnt_reconnect(fetch_schema=False)

        with self.assertRaises(TarantoolSchemaError):
            await self.conn.delete(self.TESTER_SPACE_NAME, [0])

    async def test__delete_by_index_name_no_schema(self):
        await self.tnt_reconnect(fetch_schema=False)

        with self.assertRaises(TarantoolSchemaError):
            await self.conn.delete(self.TESTER_SPACE_ID, [0],
                                   index='primary')

    async def test__delete_invalid_types(self):
        with self.assertRaisesRegex(
                TypeError,
                "missing 2 required positional arguments: 'space' and 'key'"):
            await self.conn.delete()

    async def test__delete_key_tuple(self):
        try:
            await self.conn.delete(self.TESTER_SPACE_ID, (1,))
        except Exception as e:
            self.fail(e)

    async def test__delete_dict_key(self):
        data = await self._fill_data()

        res = await self.conn.delete(self.TESTER_SPACE_ID, {
            'f1': 0
        })
        self.assertListEqual(res.body, [data[0]], 'Body ok')

    async def test__delete_dict_resp(self):
        data = [0, 'hello', 0, 1, 'wow']
        await self.conn.insert(self.TESTER_SPACE_ID, data)

        res = await self.conn.delete(self.TESTER_SPACE_ID, [0],
                                     tuple_as_dict=True)
        self.assertListEqual(res.body, [{
            'f1': 0,
            'f2': 'hello',
            'f3': 0,
            'f4': 1,
            'f5': 'wow'
        }])
