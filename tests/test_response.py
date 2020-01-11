import warnings

from asynctnt import TarantoolTuple
from tests import BaseTarantoolTestCase


class ResponseTestCase(BaseTarantoolTestCase):
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
            t = await self.conn.insert(self.TESTER_SPACE_ID, t)
            data.append(dict(t[0]))
        return data

    async def test__response_indexing(self):
        res = await self.conn.call('box.info')
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            self.assertEqual(len(res), len(res.body), 'len ok')
            self.assertEqual(res[0], res.body[0], 'value ok')

    async def test__response_iter(self):
        data = await self._fill_data(3)
        res = await self.conn.select(self.TESTER_SPACE_ID)

        self.assertEqual(len(res), len(data), 'len ok')

        res_arr = []
        for el in res:
            res_arr.append(list(el))
        self.assertListEqual(res_arr, data, 'list ok')

    async def test__response_tuple_iter(self):
        data = await self._fill_data(1)
        res = await self.conn.select(self.TESTER_SPACE_ID)
        t = res[0]

        t_list = [el for el in t]  # check iteration over tuple
        self.assertListEqual(t_list, data[0], 'tuple ok')

    async def test__response_tuple_keys(self):
        await self._fill_data(1)
        res = await self.conn.select(self.TESTER_SPACE_ID)
        t = res[0]

        correct_keys = ['f1', 'f2', 'f3', 'f4', 'f5']
        self.assertListEqual(list(t.keys()), correct_keys, 'keys ok')

    async def test__response_tuple_values(self):
        data = await self._fill_data(1)
        res = await self.conn.select(self.TESTER_SPACE_ID)
        t = res[0]

        self.assertListEqual(list(t.values()), data[0], 'values ok')

    async def test__response_tuple_items(self):
        data = [0, 'hello', 5, 6, 'help', 'common', 'yo']
        res = await self.conn.insert(self.TESTER_SPACE_ID, data)
        t = res[0]
        d = {
            'f1': data[0],
            'f2': data[1],
            'f3': data[2],
            'f4': data[3],
            'f5': data[4],
        }

        t_dict = {k: v for k, v in t.items()}
        self.assertDictEqual(t_dict, d, 'items ok')

    async def test__response_tuple_dict_extra_index(self):
        data = [0, 'hello', 5, 6, 'help', 'common', 'yo']
        res = await self.conn.insert(self.TESTER_SPACE_ID, data)
        res = res[0]

        self.assertEqual(res[0], data[0])
        self.assertEqual(res[1], data[1])
        self.assertEqual(res[2], data[2])
        self.assertEqual(res[3], data[3])
        self.assertEqual(res[4], data[4])
        self.assertEqual(res[5], data[5])
        self.assertEqual(res[6], data[6])
        self.assertEqual(res[-1], data[-1])
        self.assertEqual(res[-3], data[-3])

        self.assertEqual(res['f1'], data[0])
        self.assertEqual(res['f2'], data[1])
        self.assertEqual(res['f3'], data[2])
        self.assertEqual(res['f4'], data[3])
        self.assertEqual(res['f5'], data[4])

    async def test__response_tuple_slice(self):
        data = [0, 'hello', 5, 6, 'help', 'common', 'yo']
        res = await self.conn.insert(self.TESTER_SPACE_ID, data)
        res = res[0]

        self.assertEqual(type(res[:3]), tuple)

        self.assertListEqual(list(res[:3]), data[:3])
        self.assertListEqual(list(res[1:5]), data[1:5])
        self.assertListEqual(list(res[5:20]), data[5:20])
        self.assertEqual(list(res[7:3:-1]), data[7:3:-1])
        self.assertEqual(list(res[7:3:-2]), data[7:3:-2])

    async def test__response_tuple_contains(self):
        data = [0, 'hello', 5, 6, 'help', 'common', 'yo']
        res = await self.conn.insert(self.TESTER_SPACE_ID, data)
        res = res[0]

        self.assertTrue('f1' in res)
        self.assertTrue('f2' in res)
        self.assertTrue('f3' in res)
        self.assertTrue('f4' in res)
        self.assertTrue('f5' in res)
        self.assertFalse('f6' in res)

    async def test__response_tuple_key_error(self):
        data = [0, 'hello', 5, 6, 'help', 'common', 'yo']
        res = await self.conn.insert(self.TESTER_SPACE_ID, data)
        res = res[0]

        with self.assertRaises(KeyError):
            # noinspection PyStatementEffect
            res['f100']

    async def test__response_tuple_get(self):
        data = [0, 'hello', 5, 6, 'help', 'common', 'yo']
        res = await self.conn.insert(self.TESTER_SPACE_ID, data)
        res = res[0]

        self.assertEqual(res.get('f1'), 0)
        self.assertEqual(res.get('f2'), 'hello')
        self.assertEqual(res.get('f100'), None)
        self.assertEqual(res.get('f100', 'zz'), 'zz')

    async def test__response_with_no_space_format(self):
        res = await self.conn.insert('no_schema_space', [0, 'one'])

        with self.assertRaises(ValueError):
            res[0].keys()

        with self.assertRaises(ValueError):
            res[0].items()

    async def test__native_response_with_no_space_format(self):
        await self.conn.insert('no_schema_space', [0, 'one'])

        res = await self.conn.select('no_schema_space')
        self.assertEqual(1, res.rowcount, 'count correct')
        self.assertTrue(isinstance(res[0], TarantoolTuple),
                        'expecting a TarantoolTuple')
        self.assertResponseEqual(res, [[0, 'one']], 'resp ok')

        try:
            repr(res)
        except Exception as e:
            self.fail(e)

    async def test__response_repr(self):
        data = [0, 'hello', 5, 6, 'help', 'common', 'yo']
        await self.conn.insert(self.TESTER_SPACE_ID, data)

        res = await self.conn.select('tester')
        self.assertEqual(1, res.rowcount, 'count correct')
        self.assertTrue(isinstance(res[0], TarantoolTuple),
                        'expecting a TarantoolTuple')

        self.assertEqual(
            "<TarantoolTuple f1=0 f2='hello' f3=5 f4=6 f5='help' "
            "5='common' 6='yo'>",
            repr(res[0]), 'repr ok')

    async def test__response_str(self):
        data = [0, 'hello', 5, 6, 'help', 'common', 'yo']
        await self.conn.insert(self.TESTER_SPACE_ID, data)

        res = await self.conn.select('tester')
        self.assertEqual(1, res.rowcount, 'count correct')
        self.assertTrue(isinstance(res[0], TarantoolTuple),
                        'expecting a TarantoolTuple')

        try:
            str(res)
        except Exception as e:
            self.fail(e)
