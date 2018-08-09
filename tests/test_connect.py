import asyncio

from asynctnt.connection import ConnectionState
from asynctnt.exceptions import TarantoolDatabaseError, ErrorCode, \
    TarantoolError, TarantoolNotConnectedError
from asynctnt.instance import TarantoolSyncInstance
from tests import BaseTarantoolTestCase

import asynctnt


class ConnectTestCase(BaseTarantoolTestCase):
    DO_CONNECT = False

    async def test__connect(self):
        conn = asynctnt.Connection(host=self.tnt.host, port=self.tnt.port,
                                   reconnect_timeout=0,
                                   loop=self.loop)
        self.assertEqual(conn.state, ConnectionState.IDLE)

        await conn.connect()
        self.assertIsNotNone(conn._transport)
        self.assertIsNotNone(conn._protocol)
        self.assertTrue(conn.is_connected)
        self.assertTrue(conn._protocol.is_fully_connected())
        self.assertEqual(conn.state, ConnectionState.CONNECTED)
        self.assertIsNotNone(conn._protocol.schema)
        self.assertIsNotNone(conn.version)
        await conn.disconnect()

    async def test__connect_no_schema(self):
        conn = asynctnt.Connection(host=self.tnt.host, port=self.tnt.port,
                                   reconnect_timeout=0,
                                   fetch_schema=False,
                                   auto_refetch_schema=False,
                                   loop=self.loop)
        await conn.connect()
        self.assertIsNotNone(conn._transport)
        self.assertIsNotNone(conn._protocol)
        self.assertTrue(conn.is_connected)
        self.assertTrue(conn._protocol.is_fully_connected())
        self.assertEqual(conn.state, ConnectionState.CONNECTED)
        self.assertIsNotNone(conn._protocol.schema)
        await conn.disconnect()

    async def test__connect_auth(self):
        conn = asynctnt.Connection(host=self.tnt.host, port=self.tnt.port,
                                   username='t1', password='t1',
                                   reconnect_timeout=0,
                                   loop=self.loop)
        await conn.connect()
        self.assertIsNotNone(conn._transport)
        self.assertIsNotNone(conn._protocol)
        self.assertTrue(conn.is_connected)
        self.assertTrue(conn._protocol.is_fully_connected())
        self.assertEqual(conn.state, ConnectionState.CONNECTED)
        self.assertIsNotNone(conn._protocol.schema)
        await conn.disconnect()

    async def test__connect_auth_no_schema(self):
        conn = asynctnt.Connection(host=self.tnt.host, port=self.tnt.port,
                                   username='t1', password='t1',
                                   fetch_schema=False,
                                   auto_refetch_schema=False,
                                   reconnect_timeout=0,
                                   loop=self.loop)
        await conn.connect()
        self.assertIsNotNone(conn._transport)
        self.assertIsNotNone(conn._protocol)
        self.assertTrue(conn.is_connected)
        self.assertTrue(conn._protocol.is_fully_connected())
        self.assertEqual(conn.state, ConnectionState.CONNECTED)
        self.assertIsNotNone(conn._protocol.schema)
        await conn.disconnect()

    async def test__disconnect(self):
        conn = asynctnt.Connection(host=self.tnt.host, port=self.tnt.port,
                                   reconnect_timeout=0,
                                   loop=self.loop)
        await conn.connect()
        await conn.disconnect()
        self.assertFalse(conn.is_connected)
        self.assertEqual(conn.state, ConnectionState.DISCONNECTED)
        # self.assertIsNone(conn.schema)

    async def test__disconnect_auth(self):
        conn = asynctnt.Connection(host=self.tnt.host, port=self.tnt.port,
                                   username='t1', password='t1',
                                   reconnect_timeout=0,
                                   loop=self.loop)
        await conn.connect()
        await conn.disconnect()
        self.assertFalse(conn.is_connected)
        self.assertEqual(conn.state, ConnectionState.DISCONNECTED)
        # self.assertIsNone(conn.schema)

    async def test__connect_multiple(self):
        conn = asynctnt.Connection(host=self.tnt.host, port=self.tnt.port,
                                   fetch_schema=False,
                                   reconnect_timeout=0,
                                   loop=self.loop)
        for _ in range(10):
            await conn.connect()
            await conn.disconnect()
        self.assertFalse(conn.is_connected)
        self.assertEqual(conn.state, ConnectionState.DISCONNECTED)

    async def test__connect_cancel(self):
        conn = asynctnt.Connection(host=self.tnt.host, port=self.tnt.port,
                                   fetch_schema=True,
                                   reconnect_timeout=0,
                                   loop=self.loop)
        f = asyncio.ensure_future(conn.connect(), loop=self.loop)
        await self.sleep(0.0001)
        f.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await f

        await conn.disconnect()

    async def test__connect_wait_tnt_started(self):
        self.tnt.stop()
        conn = asynctnt.Connection(host=self.tnt.host, port=self.tnt.port,
                                   username='t1', password='t1',
                                   fetch_schema=True,
                                   reconnect_timeout=0.000000001,
                                   loop=self.loop)
        coro = self.ensure_future(conn.connect())
        await self.sleep(0.3)
        self.tnt.start()
        await self.sleep(1)
        while True:
            try:
                await coro
                break
            except TarantoolDatabaseError as e:
                if e.code == ErrorCode.ER_NO_SUCH_USER:
                    # Try again
                    coro = self.ensure_future(conn.connect())
                    continue
                raise

        self.assertEqual(conn.state, ConnectionState.CONNECTED)
        await conn.disconnect()

    async def test__connect_waiting_for_spaces(self):
        self.tnt.stop()

        tnt = TarantoolSyncInstance(
            port=TarantoolSyncInstance.get_random_port(),
            console_port=TarantoolSyncInstance.get_random_port(),
            applua=self.read_applua(),
            cleanup=self.TNT_CLEANUP
        )
        tnt.replication_source = ['x:1']
        tnt.start(wait=False)

        conn = asynctnt.Connection(host=self.tnt.host, port=self.tnt.port,
                                   fetch_schema=True,
                                   reconnect_timeout=0.1,
                                   connect_timeout=10,
                                   loop=self.loop)
        try:
            states = {}

            async def state_checker():
                while True:
                    states[conn.state] = True
                    await self.sleep(0.001)

            checker = self.ensure_future(state_checker())

            try:
                await asyncio.wait_for(conn.connect(), 1, loop=self.loop)
            except asyncio.TimeoutError:
                self.assertTrue(True, 'connect cancelled')

            checker.cancel()

            self.assertTrue(states.get(ConnectionState.CONNECTING, False),
                            'was in connecting')
            self.assertTrue(states.get(ConnectionState.RECONNECTING, False),
                            'was in reconnecting')
        finally:
            tnt.stop()
            await conn.disconnect()

    async def test__connect_tnt_restarted(self):
        conn = asynctnt.Connection(host=self.tnt.host, port=self.tnt.port,
                                   username='t1', password='t1',
                                   fetch_schema=True,
                                   reconnect_timeout=0.000001,
                                   loop=self.loop)
        await conn.connect()

        self.tnt.stop()
        self.tnt.start()
        await self.sleep(0.5)
        try:
            await conn.ping()
        except Exception as e:
            self.fail(
                'Should not throw any exceptions, but got: {}'.format(e))
        finally:
            await conn.disconnect()

    async def test__connect_force_disconnect(self):
        conn = asynctnt.Connection(host=self.tnt.host, port=44444,
                                   reconnect_timeout=0.3,
                                   loop=self.loop)
        self.ensure_future(conn.connect())
        await self.sleep(1)
        await conn.disconnect()
        self.assertEqual(conn.state, ConnectionState.DISCONNECTED)

    async def test__close(self):
        conn = asynctnt.Connection(host=self.tnt.host, port=self.tnt.port,
                                   reconnect_timeout=0,
                                   loop=self.loop)
        await conn.connect()
        await self.sleep(0.1)
        conn.close()
        await self.sleep(0.1)
        self.assertEqual(conn.state, ConnectionState.DISCONNECTED)

    async def test_reconnect_from_idle(self):
        conn = asynctnt.Connection(host=self.tnt.host, port=self.tnt.port,
                                   reconnect_timeout=0,
                                   loop=self.loop)
        await conn.reconnect()

        self.assertEqual(conn.state, ConnectionState.CONNECTED)
        await conn.disconnect()

    async def test_reconnect_after_connect(self):
        conn = asynctnt.Connection(host=self.tnt.host, port=self.tnt.port,
                                   reconnect_timeout=0,
                                   loop=self.loop)
        await conn.connect()
        await conn.reconnect()

        self.assertEqual(conn.state, ConnectionState.CONNECTED)
        await conn.disconnect()

    async def test_manual_reconnect(self):
        conn = asynctnt.Connection(host=self.tnt.host, port=self.tnt.port,
                                   reconnect_timeout=0,
                                   loop=self.loop)
        await conn.connect()
        await conn.disconnect()
        await conn.connect()

        self.assertEqual(conn.state, ConnectionState.CONNECTED)
        await conn.disconnect()

    async def test__connect_connection_lost(self):
        conn = asynctnt.Connection(host=self.tnt.host, port=self.tnt.port,
                                   reconnect_timeout=1/3,
                                   loop=self.loop)
        await conn.connect()
        self.tnt.stop()
        await self.sleep(0.5)
        self.tnt.start()
        await self.sleep(0.5)

        self.assertEqual(conn.state, ConnectionState.CONNECTED)
        self.assertTrue(conn.is_connected)

        await conn.disconnect()

    async def test__connect_tuple_as_dict_invalid(self):
        with self.assertRaisesRegex(
                TarantoolError,
                'fetch_schema must be True to be able to use '
                'unpacking tuples to dict'):
            asynctnt.Connection(host=self.tnt.host,
                                port=self.tnt.port,
                                fetch_schema=False,
                                auto_refetch_schema=False,
                                tuple_as_dict=True,
                                loop=self.loop)

    async def test__connect_from_multiple_coroutines(self):
        conn = asynctnt.Connection(host=self.tnt.host, port=self.tnt.port,
                                   reconnect_timeout=1/3,
                                   loop=self.loop)

        coros = []
        for _ in range(10):
            coros.append(asyncio.ensure_future(conn.connect(),
                                               loop=self.loop))

        await asyncio.gather(*coros, loop=self.loop)
        self.assertEqual(conn.state, ConnectionState.CONNECTED)
        self.assertTrue(conn.is_connected)
        await conn.disconnect()

    async def test__connect_while_reconnecting(self):
        conn = asynctnt.Connection(host=self.tnt.host, port=self.tnt.port,
                                   reconnect_timeout=1,
                                   loop=self.loop)

        coros = []
        for _ in range(10):
            coros.append(asyncio.ensure_future(conn.connect(),
                                               loop=self.loop))

        self.tnt.stop()
        await self.sleep(0.5)

        connect_coros = asyncio.ensure_future(
            asyncio.gather(*coros, loop=self.loop),
            loop=self.loop
        )

        self.tnt.start()
        await self.sleep(1)
        await connect_coros

        self.assertEqual(conn.state, ConnectionState.CONNECTED)
        self.assertTrue(conn.is_connected)

        await conn.disconnect()

    async def test__connect_on_tnt_crash_no_reconnect(self):
        conn = asynctnt.Connection(host=self.tnt.host, port=self.tnt.port,
                                   connect_timeout=1,
                                   reconnect_timeout=0,
                                   loop=self.loop)
        try:
            await conn.connect()
            await asyncio.sleep(0.5, loop=self.loop)
            try:
                await conn.eval("require('ffi').cast('char *', 0)[0] = 48")
            except TarantoolNotConnectedError:
                self.assertTrue(True, 'not connected error triggered')
            self.tnt.stop()
            self.tnt.start()
            await asyncio.sleep(1, loop=self.loop)
            await conn.connect()  # this connect should reconnect easily
        finally:
            await conn.disconnect()

    async def test__connect_on_tnt_crash_with_reconnect(self):
        conn = asynctnt.Connection(host=self.tnt.host, port=self.tnt.port,
                                   connect_timeout=1,
                                   reconnect_timeout=1/3,
                                   loop=self.loop)
        try:
            await conn.connect()
            await asyncio.sleep(0.5, loop=self.loop)
            try:
                await conn.eval("require('ffi').cast('char *', 0)[0] = 48")
            except TarantoolNotConnectedError:
                self.assertTrue(True, 'not connected error triggered')
            self.tnt.stop()
            self.tnt.start()
            await asyncio.sleep(1, loop=self.loop)
            await conn.connect()  # this connect should reconnect easily
        finally:
            await conn.disconnect()
