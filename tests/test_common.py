import logging

import asynctnt
from asynctnt.exceptions import TarantoolNotConnectedError, \
    TarantoolDatabaseError

from tests import BaseTarantoolTestCase
from tests.util import get_complex_param, get_big_param


class CommonTestCase(BaseTarantoolTestCase):

    async def test__encoding_utf8(self):
        p, p_cmp = get_complex_param(replace_bin=False)

        data = [1, 'hello', 1, 0, p]
        data_cmp = [1, 'hello', 1, 0, p_cmp]

        res = await self.conn.insert(self.TESTER_SPACE_ID, data)
        self.assertListEqual(res.body, [data_cmp], 'Body ok')

        res = await self.conn.select(self.TESTER_SPACE_ID)
        self.assertListEqual(res.body, [data_cmp], 'Body ok')

    async def test__encoding_cp1251(self):
        await self.tnt_reconnect(encoding='cp1251')
        p, p_cmp = get_complex_param(replace_bin=False)

        data = [1, 'hello', 1, 0, p]
        data_cmp = [1, 'hello', 1, 0, p_cmp]

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

    async def test__schema_refetch_on_schema_change_format(self):
        await self.tnt_reconnect(auto_refetch_schema=True,
                                 username='t1', password='t1')
        self.assertTrue(self.conn.fetch_schema)
        self.assertTrue(self.conn.auto_refetch_schema)
        schema_before = self.conn.schema_id
        self.assertNotEqual(schema_before, -1)

        await self.conn.call('change_format')

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

    async def test__parse_numeric_map_keys(self):
        res = await self.conn.eval(
            """return {
                [1] = 1, 
                [2] = 2, 
                hello = 3, 
                world = 4, 
                [-3] = 5, 
                [4.5] = 6
            }"""
        )

        d = {
            1: 1,
            2: 2,
            'hello': 3,
            'world': 4,
            -3: 5,
            4.5: 6
        }

        self.assertDictEqual(res.body[0], d, 'Numeric keys parsed ok')

    async def test__read_buffer_reallocate_ok(self):
        await self.tnt_reconnect(initial_read_buffer_size=1)

        p, cmp = get_complex_param(encoding=self.conn.encoding)
        try:
            res = await self.conn.call('func_param', [p])
        except Exception as e:
            self.fail(e)

        self.assertDictEqual(res.body[0][0], cmp, 'Body ok')

    async def test__read_buffer_deallocate_ok(self):
        size = 100 * 1000
        await self.tnt_reconnect(initial_read_buffer_size=size)

        # Waiting big response, so ReadBuffer grows to hold it
        p = get_big_param(size=size * 3)
        try:
            res = await self.conn.call('func_param', [p])
        except Exception as e:
            self.fail(e)

        # Waiting small response, so ReadBuffer deallocates memory
        p = get_big_param(size=10)
        try:
            res = await self.conn.call('func_param', [p])
        except Exception as e:
            self.fail(e)

    async def test__write_buffer_reallocate(self):
        p = get_big_param(size=100*1024)
        try:
            res = await self.conn.call('func_param', [p])
        except Exception as e:
            self.fail(e)

        self.assertDictEqual(res.body[0][0], p, 'Body ok')

    async def test__ensure_no_attribute_error_on_not_connected(self):
        await self.tnt_disconnect()

        self.conn = asynctnt.Connection(
            host=self.tnt.host,
            port=self.tnt.port,
            loop=self.loop)

        with self.assertRaises(TarantoolNotConnectedError):
            await self.conn.ping()

    async def test__encode_unsupported_type(self):
        class A:
            pass
        with self.assertRaisesRegex(
                TypeError, 'Type `(.+)` is not supported for encoding'):
            await self.conn.call('func_param', [{'a': A()}])

    async def test__schema_refetch_next_byte(self):
        await self.tnt_reconnect(auto_refetch_schema=True)
        await self.conn.call('func_hello')

        # Changing scheme
        try:
            for _ in range(251):
                await self.conn.eval(
                    "s = box.schema.create_space('new_space');"
                    "s:drop();"
                )
        except TarantoolDatabaseError as e:
            self.fail(e)

        try:
            for i in range(1, 255):
                await self.conn.call('func_hello')
        except TarantoolDatabaseError as e:
            self.fail(e)
