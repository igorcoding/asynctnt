import asyncio

import asynctnt
from asynctnt import Response, PushIterator
from asynctnt._testbase import ensure_version
from asynctnt.exceptions import TarantoolDatabaseError, TarantoolNotConnectedError
from tests import BaseTarantoolTestCase


class PushTestCase(BaseTarantoolTestCase):
    @ensure_version(min=(1, 10))
    async def test__push_invalid_future(self):

        with self.assertRaises(ValueError) as e:
            PushIterator(asyncio.Future())

        self.assertEqual(str(e.exception),
                         'Future is invalid. Make sure to call with '
                         'a future returned from a method with '
                         'push_subscribe=True flag')

    @ensure_version(min=(1, 10))
    async def test__push_invalid_future_no_flag(self):
        res = self.conn.call('async_action')

        with self.assertRaises(ValueError) as e:
            PushIterator(res)

        self.assertEqual(str(e.exception),
                         'Future is invalid. Make sure to call with '
                         'a future returned from a method with '
                         'push_subscribe=True flag')

    @ensure_version(min=(1, 10))
    async def test__push_correct_res(self):
        fut = self.conn.call('async_action', push_subscribe=True)
        self.assertEqual(type(fut), asyncio.Future)

        try:
            it = PushIterator(fut)
            self.assertIsInstance(it.response, asynctnt.Response)
        except Exception as e:
            self.fail(e)

    @ensure_version(min=(1, 10))
    async def test__push_call_iter(self):
        fut = self.conn.call('async_action', push_subscribe=True)

        with self.assertRaises(RuntimeError) as e:
            for _ in PushIterator(fut):
                pass
        self.assertEqual(str(e.exception),
                         'Cannot use iter with PushIterator - use aiter')

    @ensure_version(min=(1, 10))
    async def test__push_read_all(self):
        fut = self.conn.call('async_action', push_subscribe=True)
        it = PushIterator(fut)

        self.assertFalse(it.response.done(), 'response not done')

        result = []

        async for entry in it:
            result.append(entry[0])

        self.assertTrue(it.response.done(), 'response is done')

        self.assertListEqual(result, [
            'hello_1',
            'hello_2',
            'hello_3',
            'hello_4',
            'hello_5'
        ], 'push values ok')

        fut_res = await fut
        self.assertIsInstance(fut_res, Response, 'got response')
        self.assertEqual(fut_res.code, 0, 'code ok')
        self.assertEqual(fut_res.sync, it.response.sync, 'sync ok')
        self.assertEqual(fut_res.return_code, 0, 'return code ok')
        self.assertEqual(fut_res.body, ['ret'], 'return value ok')
        self.assertTrue(fut_res.done(), 'response done')

    @ensure_version(min=(1, 10))
    async def test__push_read_in_parts(self):
        fut = self.conn.call('async_action', push_subscribe=True)
        it = PushIterator(fut)

        result = []

        i = 0
        async for entry in it:
            if len(entry) == 0:
                self.fail("got 0 length for entry #{}".format(i))
            result.append(entry[0])
            i += 1
            if i == 2:
                break

        async for entry in it:
            if len(entry) == 0:
                self.fail("got 0 length for entry #{}".format(i))
            result.append(entry[0])
            i += 1

        self.assertListEqual(result, [
            'hello_1',
            'hello_2',
            'hello_3',
            'hello_4',
            'hello_5'
        ], 'push values ok')

        fut_res = await fut
        self.assertIsInstance(fut_res, Response, 'got response')
        self.assertEqual(fut_res.code, 0, 'code ok')
        self.assertEqual(fut_res.sync, it.response.sync, 'sync ok')
        self.assertEqual(fut_res.return_code, 0, 'return code ok')
        self.assertEqual(fut_res.body, ['ret'], 'return value ok')

    @ensure_version(min=(1, 10))
    async def test__push_read_all_eval(self):
        fut = self.conn.eval("""
            for i = 1, 5 do
                box.session.push('hello_' .. tostring(i))
                require'fiber'.sleep(0.01)
            end
            return 'ret'
        """, push_subscribe=True)
        it = PushIterator(fut)

        result = []

        i = 0
        async for entry in it:
            if len(entry) == 0:
                self.fail("got 0 length for entry #{}".format(i))
            result.append(entry[0])
            i += 1

        self.assertListEqual(result, [
            'hello_1',
            'hello_2',
            'hello_3',
            'hello_4',
            'hello_5'
        ], 'push values ok')

        fut_res = await fut
        self.assertIsInstance(fut_res, Response, 'got response')
        self.assertEqual(fut_res.code, 0, 'code ok')
        self.assertEqual(fut_res.sync, it.response.sync, 'sync ok')
        self.assertEqual(fut_res.return_code, 0, 'return code ok')
        self.assertEqual(fut_res.body, ['ret'], 'return value ok')

    @ensure_version(min=(1, 10))
    async def test__push_read_all_various_sleep(self):
        fut = self.conn.eval("""
            box.session.push('hello_1')
            require'fiber'.sleep(0.01)
            box.session.push('hello_2')
            require'fiber'.sleep(1)
            box.session.push('hello_3')
            return 'ret'
        """, push_subscribe=True)
        it = PushIterator(fut)

        result = []

        i = 0
        async for entry in it:
            result.append(entry[0])
            i += 1

        self.assertListEqual(result, [
            'hello_1',
            'hello_2',
            'hello_3'
        ], 'push values ok')

        fut_res = await fut
        self.assertIsInstance(fut_res, Response, 'got response')
        self.assertEqual(fut_res.code, 0, 'code ok')
        self.assertEqual(fut_res.sync, it.response.sync, 'sync ok')
        self.assertEqual(fut_res.return_code, 0, 'return code ok')
        self.assertEqual(fut_res.body, ['ret'], 'return value ok')

    @ensure_version(min=(1, 10))
    async def test__push_read_all_error(self):
        fut = self.conn.eval("""
                    for i = 1, 5 do
                        box.session.push('hello_' .. tostring(i))
                        require'fiber'.sleep(0.01)
                    end
                    return 'ret'
                """, push_subscribe=True)
        it = PushIterator(fut)

        # iter once
        await it.__anext__()

        # drop tarantool
        self.tnt.stop()

        try:
            with self.assertRaises(TarantoolNotConnectedError):
                await asyncio.wait_for(it.__anext__(), timeout=5)
        finally:
            self.tnt.start()

    @ensure_version(min=(1, 10))
    async def test__push_read_all_disconnect(self):
        fut = self.conn.eval("error('some error')", push_subscribe=True)
        it = PushIterator(fut)

        with self.assertRaises(TarantoolDatabaseError):
            await it.__anext__()

        with self.assertRaises(TarantoolDatabaseError):
            await fut

    @ensure_version(min=(1, 10))
    async def test__push_read_all_multiple_iterators(self):
        fut = self.conn.eval("box.session.push(1);"
                             "box.session.push(2);"
                             "box.session.push(3);", push_subscribe=True)
        it1 = PushIterator(fut)
        it2 = PushIterator(fut)

        async def f(it):
            results = []
            async for entry in it:
                results.append(entry[0])

            return results

        res1, res2 = await asyncio.gather(f(it1), f(it2))
        res1.extend(res2)
        res1.sort()

        self.assertListEqual(res1, [1, 2, 3])

    @ensure_version(min=(1, 10))
    async def test__push_read_all_one_iterator(self):
        fut = self.conn.eval("box.session.push('hello_1');"
                             "box.session.push('hello_2');"
                             "box.session.push('hello_3');",
                             push_subscribe=True)

        it = PushIterator(fut)
        results = []
        async for entry in it:
            results.append(entry[0])

        self.assertListEqual(results, [
            'hello_1',
            'hello_2',
            'hello_3',
        ], 'push ok')
