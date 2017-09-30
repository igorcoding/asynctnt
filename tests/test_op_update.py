import asyncio

import logging

from asynctnt import Iterator
from asynctnt import Response
from asynctnt.exceptions import TarantoolSchemaError, TarantoolDatabaseError, \
    ErrorCode
from tests import BaseTarantoolTestCase
from tests.util import get_complex_param


class UpdateTestCase(BaseTarantoolTestCase):
    LOGGING_LEVEL = logging.DEBUG

    async def _fill_data(self):
        data = [
            [0, 'a', 1, 5, 'data1'],
            [1, 'b', 8, 6, 'data2'],
            [2, 'c', 10, 12, 'data3', 'extra_field'],
        ]
        for t in data:
            await self.conn.insert(self.TESTER_SPACE_ID, t)

        return data

    async def test__update_one_assign(self):
        data = await self._fill_data()

        res = await self.conn.update(self.TESTER_SPACE_ID,
                                     [1], [['=', 2, 2]])
        self.assertIsInstance(res, Response, 'Got response')
        self.assertEqual(res.code, 0, 'success')
        self.assertGreater(res.sync, 0, 'sync > 0')

        data[1][2] = 2
        self.assertListEqual(res.body, [data[1]], 'Body ok')

    async def test__update_one_insert(self):
        data = await self._fill_data()

        res = await self.conn.update(self.TESTER_SPACE_ID,
                                     [1], [['!', 2, 14]])
        data[1].insert(2, 14)
        self.assertListEqual(res.body, [data[1]], 'Body ok')

    async def test__update_one_delete(self):
        data = await self._fill_data()

        res = await self.conn.update(self.TESTER_SPACE_ID,
                                     [2], [['#', 5, 1]])
        data[2].pop(5)
        self.assertListEqual(res.body, [data[2]], 'Body ok')

    async def test__update_one_plus(self):
        data = await self._fill_data()

        res = await self.conn.update(self.TESTER_SPACE_ID,
                                     [1], [['+', 2, 3]])
        data[1][2] += 3
        self.assertListEqual(res.body, [data[1]], 'Body ok')

    async def test__update_one_plus_str_field(self):
        data = await self._fill_data()

        res = await self.conn.update(self.TESTER_SPACE_ID,
                                     [1], [['+', 'f3', 3]])
        data[1][2] += 3
        self.assertListEqual(res.body, [data[1]], 'Body ok')

    async def test__update_one_plus_str_field_unknown(self):
        data = await self._fill_data()

        with self.assertRaisesRegex(TarantoolSchemaError,
                                    r'Field with name \'f10\' not found '
                                    r'in space \'tester\''):
            await self.conn.update(self.TESTER_SPACE_ID,
                                   [1], [['+', 'f10', 3]])

    async def test__update_one_plus_negative(self):
        data = await self._fill_data()

        res = await self.conn.update(self.TESTER_SPACE_ID,
                                     [1], [['+', 2, -3]])
        data[1][2] += -3
        self.assertListEqual(res.body, [data[1]], 'Body ok')

    async def test__update_one_minus(self):
        data = await self._fill_data()

        res = await self.conn.update(self.TESTER_SPACE_ID,
                                     [0], [['-', 2, 1]])
        data[0][2] -= 1
        self.assertListEqual(res.body, [data[0]], 'Body ok')

    async def test__update_one_minus_negative(self):
        data = await self._fill_data()

        res = await self.conn.update(self.TESTER_SPACE_ID,
                                     [1], [['-', 2, -3]])
        data[1][2] -= -3
        self.assertListEqual(res.body, [data[1]], 'Body ok')

    async def test__update_one_band(self):
        data = await self._fill_data()

        res = await self.conn.update(self.TESTER_SPACE_ID,
                                     [1], [['&', 2, 3]])
        data[1][2] &= 3
        self.assertListEqual(res.body, [data[1]], 'Body ok')

        res = await self.conn.update(self.TESTER_SPACE_ID,
                                     [1], [['&', 2, 2]])
        data[1][2] &= 2
        self.assertListEqual(res.body, [data[1]], 'Body ok')

        res = await self.conn.update(self.TESTER_SPACE_ID,
                                     [1], [['&', 2, 1]])
        data[1][2] &= 1
        self.assertListEqual(res.body, [data[1]], 'Body ok')

        res = await self.conn.update(self.TESTER_SPACE_ID,
                                     [1], [['&', 2, 0]])
        data[1][2] &= 0
        self.assertListEqual(res.body, [data[1]], 'Body ok')

    async def test__update_one_bor(self):
        data = await self._fill_data()

        res = await self.conn.update(self.TESTER_SPACE_ID,
                                     [1], [['|', 2, 3]])
        data[1][2] |= 3
        self.assertListEqual(res.body, [data[1]], 'Body ok')

        res = await self.conn.update(self.TESTER_SPACE_ID,
                                     [1], [['|', 2, 2]])
        data[1][2] |= 2
        self.assertListEqual(res.body, [data[1]], 'Body ok')

        res = await self.conn.update(self.TESTER_SPACE_ID,
                                     [1], [['|', 2, 1]])
        data[1][2] |= 1
        self.assertListEqual(res.body, [data[1]], 'Body ok')

        res = await self.conn.update(self.TESTER_SPACE_ID,
                                     [1], [['|', 2, 0]])
        data[1][2] |= 0
        self.assertListEqual(res.body, [data[1]], 'Body ok')

    async def test__update_one_bxor(self):
        data = await self._fill_data()

        res = await self.conn.update(self.TESTER_SPACE_ID,
                                     [1], [['^', 2, 3]])
        data[1][2] ^= 3
        self.assertListEqual(res.body, [data[1]], 'Body ok')

        res = await self.conn.update(self.TESTER_SPACE_ID,
                                     [1], [['^', 2, 2]])
        data[1][2] ^= 2
        self.assertListEqual(res.body, [data[1]], 'Body ok')

        res = await self.conn.update(self.TESTER_SPACE_ID,
                                     [1], [['^', 2, 1]])
        data[1][2] ^= 1
        self.assertListEqual(res.body, [data[1]], 'Body ok')

        res = await self.conn.update(self.TESTER_SPACE_ID,
                                     [1], [['^', 2, 0]])
        data[1][2] ^= 0
        self.assertListEqual(res.body, [data[1]], 'Body ok')

    async def test__update_splice(self):
        data = [1, 'hello2', 1, 4, 'what is up']
        await self.conn.insert(self.TESTER_SPACE_ID, data)

        res = await self.conn.update(self.TESTER_SPACE_ID,
                                     [1], [[':', 1, 1, 3, '!!!']])

        data[1] = 'h!!!o2'
        self.assertListEqual(res.body, [data], 'Body ok')

    async def test__update_splice_bytes(self):
        data = [1, 'hello2', 1, 4, 'what is up']
        await self.conn.insert(self.TESTER_SPACE_ID, data)

        res = await self.conn.update(self.TESTER_SPACE_ID,
                                     [1], [[b':', 1, 1, 3, '!!!']])

        data[1] = 'h!!!o2'
        self.assertListEqual(res.body, [data], 'Body ok')

    async def test__update_splice_wrong_args(self):
        data = [1, 'hello2', 1, 4, 'what is up']
        await self.conn.insert(self.TESTER_SPACE_ID, data)

        with self.assertRaisesRegex(
                IndexError, r'Operation length must be at least 3'):
            await self.conn.update(self.TESTER_SPACE_ID,
                                   [1], [[':', 2]])

        with self.assertRaisesRegex(
                IndexError, r'Splice operation must have length of 5'):
            await self.conn.update(self.TESTER_SPACE_ID,
                                   [1], [[':', 2, 1]])

        with self.assertRaisesRegex(
                IndexError, r'Splice operation must have length of 5'):
            await self.conn.update(self.TESTER_SPACE_ID,
                                   [1], [[':', 2, 1, 3]])

        with self.assertRaisesRegex(
                TypeError, r'Splice offset must be int'):
            await self.conn.update(self.TESTER_SPACE_ID,
                                   [1], [[':', 2, 1, {}, ':::']])

        with self.assertRaisesRegex(
                TypeError, r'Splice position must be int'):
            await self.conn.update(self.TESTER_SPACE_ID,
                                   [1], [[':', 2, {}, {}, ':::']])

        with self.assertRaisesRegex(
                TypeError, r'Operation field_no must be '
                           r'of either int or str type'):
            await self.conn.update(self.TESTER_SPACE_ID,
                                   [1], [[':', {}, {}, {}, ':::']])

        with self.assertRaisesRegex(
                TypeError, r'Unknown update operation type `yo`'):
            await self.conn.update(self.TESTER_SPACE_ID,
                                   [1], [['yo', 1, 2, 3]])

        with self.assertRaisesRegex(
                TypeError, r'Operation type must of a str or bytes type'):
            await self.conn.update(self.TESTER_SPACE_ID,
                                   [1], [[{}, 1, 2, 3]])

        with self.assertRaisesRegex(
                TypeError, r'Single operation must be a tuple or list'):
            await self.conn.update(self.TESTER_SPACE_ID,
                                   [1], [{}])

        with self.assertRaisesRegex(
                TypeError, r'int argument required for '
                           r'Arithmetic and Delete operations'):
            await self.conn.update(self.TESTER_SPACE_ID,
                                   [1], [('+', 2, {})])

    async def test__update_multiple_operations(self):
        t = [1, '1', 1, 5, 'hello', 3, 4, 8]
        await self.conn.insert(self.TESTER_SPACE_ID, t)

        t[2] += 1
        t[3] -= 4
        t[5] &= 5
        t[6] |= 7
        t[7] = 100
        t[4] = 'h!!!o'

        operations = [
            ['+', 2, 1],
            ['-', 3, 4],
            ['&', 5, 5],
            ['|', 6, 7],
            ['=', 7, 100],
            [':', 4, 1, 3, '!!!'],
        ]

        res = await self.conn.update(self.TESTER_SPACE_ID,
                                     [1], operations)
        self.assertListEqual(res.body, [t], 'Body ok')

    async def test__update_by_name(self):
        data = await self._fill_data()

        res = await self.conn.update(self.TESTER_SPACE_NAME,
                                     [1], [['=', 2, 2]])

        data[1][2] = 2
        self.assertIsInstance(res, Response, 'Got response')
        self.assertEqual(res.code, 0, 'success')
        self.assertGreater(res.sync, 0, 'sync > 0')
        self.assertListEqual(res.body, [data[1]], 'Body ok')

    async def test__update_by_name_no_schema(self):
        await self._fill_data()

        await self.tnt_reconnect(fetch_schema=False)

        with self.assertRaises(TarantoolSchemaError):
            await self.conn.update(self.TESTER_SPACE_NAME,
                                   [1], [['=', 2, 2]])

    async def test__update_by_index_id(self):
        index_name = 'temp_idx'
        res = self.tnt.command(
            'make_third_index("{}")'.format(index_name)
        )
        index_id = res[0][0]

        try:
            await self.tnt_reconnect()
            data = await self._fill_data()

            res = await self.conn.update(self.TESTER_SPACE_ID, [data[0][2]],
                                         [('=', 2, 1)], index=index_id)

            data[0][2] = 1
            self.assertIsInstance(res, Response, 'Got response')
            self.assertEqual(res.code, 0, 'success')
            self.assertGreater(res.sync, 0, 'sync > 0')
            self.assertListEqual(res.body, [data[0]], 'Body ok')
        finally:
            self.tnt.command(
                'box.space.{}.index.{}:drop()'.format(
                    self.TESTER_SPACE_NAME, index_name)
            )

    async def test__select_by_index_name(self):
        index_name = 'temp_idx'
        res = self.tnt.command(
            'make_third_index("{}")'.format(index_name)
        )
        index_id = res[0][0]

        try:
            await self.tnt_reconnect()
            data = await self._fill_data()

            res = await self.conn.update(self.TESTER_SPACE_ID, [data[0][2]],
                                         [['=', 2, 1]], index=index_name)

            data[0][2] = 1
            self.assertIsInstance(res, Response, 'Got response')
            self.assertEqual(res.code, 0, 'success')
            self.assertGreater(res.sync, 0, 'sync > 0')
            self.assertListEqual(res.body, [data[0]], 'Body ok')
        finally:
            self.tnt.command(
                'box.space.{}.index.{}:drop()'.format(
                    self.TESTER_SPACE_NAME, index_name)
            )

    async def test__update_by_index_id_no_schema(self):
        await self._fill_data()
        await self.tnt_reconnect(fetch_schema=False)

        try:
            await self.conn.update(self.TESTER_SPACE_ID, [0],
                                   [['=', 2, 1]], index=0)
        except Exception as e:
            self.fail(e)

    async def test__update_by_index_name_no_schema(self):
        await self.tnt_reconnect(fetch_schema=False)

        with self.assertRaises(TarantoolSchemaError):
            await self.conn.update(self.TESTER_SPACE_NAME, [0],
                                   [['=', 2, 1]], index='primary')

    async def test__update_operations_none(self):
        data = await self._fill_data()
        try:
            res = await self.conn.update(self.TESTER_SPACE_NAME,
                                         [data[0][0]], None)
        except TarantoolDatabaseError as e:
            if self.conn.version < (1, 7):
                if e.code == ErrorCode.ER_ILLEGAL_PARAMS:
                    # success
                    return
            raise
        self.assertListEqual(res.body, [data[0]], 'empty operations')

    async def test__update_dict_key(self):
        data = await self._fill_data()

        res = await self.conn.update(self.TESTER_SPACE_ID, {
            'f1': 0
        }, [['+', 3, 1]])
        data[0][3] += 1
        self.assertListEqual(res.body, [data[0]], 'Body ok')

    async def test__update_dict_resp(self):
        data = await self._fill_data()

        res = await self.conn.update(self.TESTER_SPACE_ID, [0], [['+', 3, 1]],
                                     tuple_as_dict=True)
        data[0][3] += 1

        self.assertListEqual(res.body, [{
            'f1': data[0][0],
            'f2': data[0][1],
            'f3': data[0][2],
            'f4': data[0][3],
            'f5': data[0][4],
        }])
