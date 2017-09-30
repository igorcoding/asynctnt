import asyncio

import logging

from asynctnt import Iterator
from asynctnt import Response
from asynctnt.exceptions import TarantoolSchemaError
from tests import BaseTarantoolTestCase
from tests.util import get_complex_param


class SelectTestCase(BaseTarantoolTestCase):
    LOGGING_LEVEL = logging.INFO
    async def _fill_data(self, count=3):
        data = []
        for i in range(count):
            t = [i, str(i), 1, 2, 'something']
            data.append(t)
            await self.conn.insert(self.TESTER_SPACE_ID, t)
        return data

    async def _fill_data_dict(self, count=3):
        data = []
        for i in range(count):
            t = {
                'f1': i,
                'f2': str(i),
                'f3': 1,
                'f4': 2,
                'f5': 'something',
            }
            t = await self.conn.insert(self.TESTER_SPACE_ID, t,
                                       tuple_as_dict=True)
            data.append(t.body[0])
        return data

    async def test__select_by_id_empty_space(self):
        res = await self.conn.select(self.TESTER_SPACE_ID)

        self.assertIsInstance(res, Response, 'Got response')
        self.assertEqual(res.code, 0, 'success')
        self.assertGreater(res.sync, 0, 'sync > 0')
        self.assertListEqual(res.body, [], 'Body ok')

    async def test__select_by_id_non_empty_space(self):
        data = await self._fill_data()

        res = await self.conn.select(self.TESTER_SPACE_ID)

        self.assertIsInstance(res, Response, 'Got response')
        self.assertEqual(res.code, 0, 'success')
        self.assertGreater(res.sync, 0, 'sync > 0')
        self.assertListEqual(res.body, data, 'Body ok')

    async def test__select_by_name_space_empty(self):
        res = await self.conn.select(self.TESTER_SPACE_NAME)

        self.assertIsInstance(res, Response, 'Got response')
        self.assertEqual(res.code, 0, 'success')
        self.assertGreater(res.sync, 0, 'sync > 0')
        self.assertListEqual(res.body, [], 'Body ok')

    async def test__select_by_name_non_empty_space(self):
        data = await self._fill_data()

        res = await self.conn.select(self.TESTER_SPACE_NAME)

        self.assertIsInstance(res, Response, 'Got response')
        self.assertEqual(res.code, 0, 'success')
        self.assertGreater(res.sync, 0, 'sync > 0')
        self.assertListEqual(res.body, data, 'Body ok')

    async def test__select_by_index_id(self):
        data = await self._fill_data()

        res = await self.conn.select(self.TESTER_SPACE_ID, index=1)

        self.assertIsInstance(res, Response, 'Got response')
        self.assertEqual(res.code, 0, 'success')
        self.assertGreater(res.sync, 0, 'sync > 0')
        self.assertListEqual(res.body, data, 'Body ok')

    async def test__select_by_index_name(self):
        data = await self._fill_data()

        res = await self.conn.select(self.TESTER_SPACE_ID, index='txt')

        self.assertIsInstance(res, Response, 'Got response')
        self.assertEqual(res.code, 0, 'success')
        self.assertGreater(res.sync, 0, 'sync > 0')
        self.assertListEqual(res.body, data, 'Body ok')

    async def test__select_by_id_no_schema(self):
        await self.tnt_reconnect(fetch_schema=False)

        try:
            await self.conn.select(self.TESTER_SPACE_ID)
        except Exception as e:
            self.fail(e)

    async def test__select_by_name_no_schema(self):
        await self.tnt_reconnect(fetch_schema=False)

        with self.assertRaises(TarantoolSchemaError):
            await self.conn.select(self.TESTER_SPACE_NAME)

    async def test__select_by_index_id_no_schema(self):
        await self.tnt_reconnect(fetch_schema=False)

        try:
            await self.conn.select(self.TESTER_SPACE_ID, index=1)
        except Exception as e:
            self.fail(e)

    async def test__select_by_index_name_no_schema(self):
        await self.tnt_reconnect(fetch_schema=False)

        with self.assertRaises(TarantoolSchemaError):
            await self.conn.select(self.TESTER_SPACE_NAME, index='txt')

    async def test__select_by_key_one_item(self):
        data = await self._fill_data()

        res = await self.conn.select(self.TESTER_SPACE_NAME, [1])
        self.assertListEqual(res.body, [data[1]], 'Body ok')

    async def test__select_by_key_multiple_items_index(self):
        data = await self._fill_data()
        next_id = data[-1][0] + 1
        next_txt = data[-1][1]
        await self.conn.insert(self.TESTER_SPACE_ID,
                               [next_id, next_txt, 1, 2, 'text'])
        data.append([next_id, next_txt, 1, 2, 'text'])

        res = await self.conn.select(self.TESTER_SPACE_NAME, [next_txt],
                                     index='txt')
        self.assertListEqual(res.body, data[len(data)-2:], 'Body ok')

    async def test__select_limit(self):
        data = await self._fill_data()

        res = await self.conn.select(self.TESTER_SPACE_NAME, limit=1)
        self.assertListEqual(res.body, [data[0]], 'Body ok')

    async def test__select_limit_offset(self):
        data = await self._fill_data(4)

        res = await self.conn.select(self.TESTER_SPACE_NAME,
                                     limit=1, offset=2)
        self.assertListEqual(res.body, [data[2]], 'Body ok')

    async def test__select_iterator_class(self):
        data = await self._fill_data(4)

        res = await self.conn.select(self.TESTER_SPACE_NAME,
                                     iterator=Iterator.GE)
        self.assertListEqual(res.body, data, 'Body ok')

        res = await self.conn.select(self.TESTER_SPACE_NAME,
                                     iterator=Iterator.LE)
        self.assertListEqual(res.body, list(reversed(data)), 'Body ok')

    async def test__select_iterator_int(self):
        data = await self._fill_data(4)

        res = await self.conn.select(self.TESTER_SPACE_NAME,
                                     iterator=4)
        self.assertListEqual(res.body, list(reversed(data)), 'Body ok')

    async def test__select_iterator_str(self):
        data = await self._fill_data(4)

        res = await self.conn.select(self.TESTER_SPACE_NAME,
                                     iterator='LE')
        self.assertListEqual(res.body, list(reversed(data)), 'Body ok')

    async def test__select_complex(self):
        p, p_cmp = get_complex_param(replace_bin=False)
        data = [1, 'hello2', 1, 4, p_cmp]

        await self.conn.insert(self.TESTER_SPACE_ID, data)

        res = await self.conn.select(self.TESTER_SPACE_ID)
        self.assertListEqual(res.body, [data], 'Body ok')

    async def test__select_all_params(self):
        data = await self._fill_data(10)

        res = await self.conn.select(self.TESTER_SPACE_NAME,
                                     index='primary',
                                     limit=2, offset=1,
                                     iterator=Iterator.LE)
        self.assertListEqual(res.body, list(reversed(data))[1:3], 'Body ok')

    async def test__select_key_tuple(self):
        try:
            await self.conn.select(self.TESTER_SPACE_ID, (1,))
        except Exception as e:
            self.fail(e)

    async def test__select_invalid_types(self):
        with self.assertRaisesRegex(
                TypeError,
                r'missing 1 required positional argument: \'space\''):
            await self.conn.select()

        with self.assertRaisesRegex(
                TypeError,
                r'sequence must be either list, tuple or dict'):
            await self.conn.select(self.TESTER_SPACE_ID, 1)

        with self.assertRaisesRegex(
                TypeError, r'Index must be either str or int, got'):
            await self.conn.select(self.TESTER_SPACE_ID, [1],
                                   index=[1, 2])

        with self.assertRaisesRegex(
                TypeError, r'an integer is required'):
            await self.conn.select(self.TESTER_SPACE_ID, [1],
                                   index=1, limit='hello')

        with self.assertRaisesRegex(
                TypeError, r'an integer is required'):
            await self.conn.select(self.TESTER_SPACE_ID, [1],
                                   index=1, limit=1, offset='hello')

        with self.assertRaisesRegex(
                TypeError, r'Iterator is of unsupported type'):
            await self.conn.select(self.TESTER_SPACE_ID, [1],
                                   index=1, limit=1, offset=1,
                                   iterator=[1, 2])

    async def test__select_dict_key(self):
        data = await self._fill_data()
        res = await self.conn.select(self.TESTER_SPACE_ID, {
            'f1': data[0][0]
        })
        self.assertListEqual(res.body, [data[0]], 'Body ok')

    async def test__select_dict_key_wrong_field(self):
        data = await self._fill_data()
        res = await self.conn.select(self.TESTER_SPACE_ID, {
            'f2': data[0][0]
        })
        self.assertListEqual(res.body, data, 'Body ok')

    async def test__select_dict_key_other_index(self):
        data = await self._fill_data()
        res = await self.conn.select(self.TESTER_SPACE_ID, {
            'f2': data[0][1]
        }, index='txt')
        self.assertListEqual(res.body, [data[0]], 'Body ok')

    async def test__select_dict_resp(self):
        data = await self._fill_data_dict()
        res = await  self.conn.select(self.TESTER_SPACE_ID, [],
                                      tuple_as_dict=True)
        self.assertListEqual(res.body, data)

    async def test__select_dict_resp_default_from_conn_true(self):
        await self.tnt_reconnect(tuple_as_dict=True)
        data = await self._fill_data()
        res = await self.conn.select(self.TESTER_SPACE_ID, [],
                                     tuple_as_dict=False)
        self.assertListEqual(res.body, data)

    async def test__select_dict_resp_default_from_conn_false(self):
        await self.tnt_reconnect(tuple_as_dict=False)
        data = await self._fill_data_dict()
        res = await self.conn.select(self.TESTER_SPACE_ID, [],
                                     tuple_as_dict=True)
        self.assertListEqual(res.body, data)
