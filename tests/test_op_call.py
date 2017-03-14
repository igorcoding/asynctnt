import asyncio

import logging

from asynctnt import Response
from asynctnt.exceptions import TarantoolDatabaseError, ErrorCode
from tests.util import get_complex_param
from tests import BaseTarantoolTestCase


class CallTestCase(BaseTarantoolTestCase):
    LOGGING_LEVEL = logging.DEBUG

    def has_new_call(self):
        return self.conn.version >= (1, 7)

    async def test__call_basic(self):
        res = await self.conn.call('func_hello')

        self.assertIsInstance(res, Response, 'Got call response')
        self.assertEqual(res.code, 0, 'success')
        self.assertGreater(res.sync, 0, 'sync > 0')
        self.assertListEqual(res.body, [['hello']], 'Body ok')

    async def test__call_basic_bare(self):
        res = await self.conn.call('func_hello_bare')
        cmp = ['hello']

        self.assertIsInstance(res, Response, 'Got call response')
        self.assertEqual(res.code, 0, 'success')
        self.assertGreater(res.sync, 0, 'sync > 0')
        if not self.has_new_call():
            cmp = [cmp]
        self.assertListEqual(res.body, cmp, 'Body ok')

    async def test__call_unknown_function(self):
        with self.assertRaises(TarantoolDatabaseError) as ctx:
            await self.conn.call('blablabla')
        self.assertEqual(ctx.exception.code, ErrorCode.ER_NO_SUCH_PROC)

    async def test__call_with_param(self):
        res = await self.conn.call('func_param', ['myparam'])

        self.assertIsInstance(res, Response, 'Got call response')
        self.assertListEqual(res.body, [['myparam']], 'Body ok')

    async def test__call_with_param_bare(self):
        res = await self.conn.call('func_param_bare', ['myparam'])
        cmp = ['myparam']
        if not self.has_new_call():
            cmp = [cmp]

        self.assertIsInstance(res, Response, 'Got call response')
        self.assertListEqual(res.body, cmp, 'Body ok')

    async def test__call_func_name_invalid_type(self):
        with self.assertRaises(TypeError):
            await self.conn.call(12)

        with self.assertRaises(TypeError):
            await self.conn.call([1, 2])

        with self.assertRaises(TypeError):
            await self.conn.call({'a': 1})

        with self.assertRaises(TypeError):
            await self.conn.call(b'qwer')

    async def test__call_params_invalid_type(self):
        with self.assertRaises(TypeError):
            await self.conn.call('func_param', 220349)

        with self.assertRaises(TypeError):
            await self.conn.call('func_param', 'hey')

        with self.assertRaises(TypeError):
            await self.conn.call('func_param', {1: 1, 2: 2})

    async def test__call_args_tuple(self):
        try:
            await self.conn.call('func_param', (1, 2))
        except Exception as e:
            self.fail(e)

    async def test__call_complex_param(self):
        p, cmp = get_complex_param(encoding=self.conn.encoding)
        res = await self.conn.call('func_param', [p])
        self.assertDictEqual(res.body[0][0], cmp, 'Body ok')

    async def test__call_complex_param_bare(self):
        p, cmp = get_complex_param(encoding=self.conn.encoding)
        cmp = [cmp]
        res = await self.conn.call('func_param_bare', [p])
        if not self.has_new_call():
            cmp = [cmp]
        self.assertListEqual(res.body, cmp, 'Body ok')

    async def test__call_timeout_in_time(self):
        try:
            await self.conn.call('func_long', [0.1], timeout=1)
        except Exception as e:
            self.fail(e)

    async def test__call_timeout_late(self):
        with self.assertRaises(asyncio.TimeoutError):
            await self.conn.call('func_long', [0.3], timeout=0.1)

    async def test__call_raise(self):
        with self.assertRaises(TarantoolDatabaseError) as e:
            await self.conn.call('raise')

        self.assertEqual(e.exception.code, 0, 'code by box.error{} is 0')
        self.assertEqual(e.exception.message, 'my reason', 'Reason ok')


class Call16TestCase(BaseTarantoolTestCase):
    async def test__call16_basic(self):
        res = await self.conn.call16('func_hello')

        self.assertIsInstance(res, Response, 'Got call response')
        self.assertEqual(res.code, 0, 'success')
        self.assertGreater(res.sync, 0, 'sync > 0')
        self.assertListEqual(res.body, [['hello']], 'Body ok')

    async def test__call16_basic_bare(self):
        # Tarantool automatically wraps return result into tuple

        res = await self.conn.call16('func_hello')

        self.assertIsInstance(res, Response, 'Got call response')
        self.assertEqual(res.code, 0, 'success')
        self.assertGreater(res.sync, 0, 'sync > 0')
        self.assertListEqual(res.body, [['hello']], 'Body ok')

    async def test__call16_unknown_function(self):
        with self.assertRaises(TarantoolDatabaseError) as ctx:
            await self.conn.call16('blablabla')
        self.assertEqual(ctx.exception.code, ErrorCode.ER_NO_SUCH_PROC)

    async def test__call16_with_param(self):
        res = await self.conn.call16('func_param', ['myparam'])

        self.assertIsInstance(res, Response, 'Got call response')
        self.assertListEqual(res.body, [['myparam']], 'Body ok')

    async def test__call16_with_param_bare(self):
        # Tarantool automatically wraps return result into tuple

        res = await self.conn.call16('func_param_bare', ['myparam'])

        self.assertIsInstance(res, Response, 'Got call response')
        self.assertListEqual(res.body, [['myparam']], 'Body ok')

    async def test__call16_func_name_invalid_type(self):
        with self.assertRaises(TypeError):
            await self.conn.call16(12)

        with self.assertRaises(TypeError):
            await self.conn.call16([1, 2])

        with self.assertRaises(TypeError):
            await self.conn.call16({'a': 1})

        with self.assertRaises(TypeError):
            await self.conn.call16(b'qwer')

    async def test__call16_params_invalid_type(self):
        with self.assertRaises(TypeError):
            await self.conn.call16('func_param', 220349)

        with self.assertRaises(TypeError):
            await self.conn.call16('func_param', 'hey')

        with self.assertRaises(TypeError):
            await self.conn.call16('func_param', {1: 1, 2: 2})

    async def test__call16_args_tuple(self):
        try:
            await self.conn.call16('func_param', (1, 2))
        except Exception as e:
            self.fail(e)

    async def test__call16_complex_param(self):
        p, cmp = get_complex_param(encoding=self.conn.encoding)
        res = await self.conn.call('func_param', [p])
        self.assertDictEqual(res.body[0][0], cmp, 'Body ok')

    async def test__call16_complex_param_bare(self):
        p, cmp = get_complex_param(encoding=self.conn.encoding)
        res = await self.conn.call16('func_param_bare', [p])
        self.assertDictEqual(res.body[0][0], cmp, 'Body ok')

    async def test__call16_timeout_in_time(self):
        try:
            await self.conn.call16('func_long', [0.1], timeout=1)
        except Exception as e:
            self.fail(e)

    async def test__call_timeout_late(self):
        with self.assertRaises(asyncio.TimeoutError):
            await self.conn.call16('func_long', [0.3], timeout=0.1)
