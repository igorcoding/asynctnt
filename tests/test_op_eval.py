import asyncio

from asynctnt import Response
from tests.util import get_complex_param
from tests import BaseTarantoolTestCase


class EvalTestCase(BaseTarantoolTestCase):
    async def test__eval_basic(self):
        res = await self.conn.eval('return "hola"')

        self.assertIsInstance(res, Response, 'Got eval response')
        self.assertEqual(res.code, 0, 'success')
        self.assertGreater(res.sync, 0, 'sync > 0')
        self.assertResponseEqual(res, ['hola'], 'Body ok')

    async def test__eval_basic_pack(self):
        res = await self.conn.eval('return {"hola"}')

        self.assertIsInstance(res, Response, 'Got eval response')
        self.assertEqual(res.code, 0, 'success')
        self.assertGreater(res.sync, 0, 'sync > 0')
        self.assertResponseEqual(res, [['hola']], 'Body ok')

    async def test__eval_with_param(self):
        args = [1, 2, 3, 'hello']
        res = await self.conn.eval('return ...', args)

        self.assertResponseEqual(res, args, 'Body ok')

    async def test__eval_with_param_pack(self):
        args = [1, 2, 3, 'hello']
        res = await self.conn.eval('return {...}', args)

        self.assertResponseEqual(res, [args], 'Body ok')

    async def test__eval_func_name_invalid_type(self):
        with self.assertRaises(TypeError):
            await self.conn.eval(12)

        with self.assertRaises(TypeError):
            await self.conn.eval([1, 2])

        with self.assertRaises(TypeError):
            await self.conn.eval({'a': 1})

        with self.assertRaises(TypeError):
            await self.conn.eval(b'qwer')

    async def test__eval_params_invalid_type(self):
        with self.assertRaises(TypeError):
            await self.conn.eval('return {...}', 220349)

        with self.assertRaises(TypeError):
            await self.conn.eval('return {...}', 'hey')

        with self.assertRaises(TypeError):
            await self.conn.eval('return {...}', {1: 1, 2: 2})

    async def test__eval_args_tuple(self):
        try:
            await self.conn.eval('return {...}', (1, 2))
        except Exception as e:
            self.fail(e)

    async def test__eval_complex_param(self):
        p, cmp = get_complex_param(encoding=self.conn.encoding)
        res = await self.conn.eval('return {...}', [p])
        self.assertDictEqual(res[0][0], cmp, 'Body ok')

    async def test__eval_timeout_in_time(self):
        try:
            cmd = """
            local args = {...}
            local fiber = require("fiber")
            fiber.sleep(args[1])
            """
            await self.conn.eval(cmd, [0.1], timeout=1)
        except Exception as e:
            self.fail(e)

    async def test__eval_timeout_late(self):
        cmd = """
        local args = {...}
        local fiber = require("fiber")
        fiber.sleep(args[1])
        """
        with self.assertRaises(asyncio.TimeoutError):
            await self.conn.eval(cmd, [0.3], timeout=0.1)
