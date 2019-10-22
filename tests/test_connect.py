import asyncio
import uuid

import asynctnt
from asynctnt._testbase import check_version
from asynctnt.connection import ConnectionState
from asynctnt.exceptions import TarantoolDatabaseError, ErrorCode, \
    TarantoolNotConnectedError
from asynctnt.instance import TarantoolSyncInstance
from tests import BaseTarantoolTestCase


class ConnectTestCase(BaseTarantoolTestCase):
    DO_CONNECT = False

    async def test__connect(self):
        conn = asynctnt.Connection(host=self.tnt.host, port=self.tnt.port,
                                   reconnect_timeout=0)
        self.assertEqual(conn.host, self.tnt.host)
        self.assertEqual(conn.port, self.tnt.port)
        self.assertIsNone(conn.username)
        self.assertIsNone(conn.password)
        self.assertEqual(conn.reconnect_timeout, 0)
        self.assertEqual(conn.connect_timeout, 3)
        self.assertEqual(conn.loop, self.loop)
        self.assertIsNone(conn.initial_read_buffer_size)
        self.assertIsNone(conn.schema_id)
        self.assertIsNone(conn.version)
        self.assertEqual(
            repr(conn),
            "<asynctnt.Connection host={} port={} state={}>".format(
                conn.host,
                conn.port,
                repr(conn.state)
            ))
        self.assertEqual(conn.state, ConnectionState.DISCONNECTED)

        c = await conn.connect()
        self.assertEqual(c, conn)
        self.assertIsNotNone(conn._transport)
        self.assertIsNotNone(conn._protocol)
        self.assertTrue(conn.is_connected)
        self.assertTrue(conn.is_fully_connected)
        self.assertEqual(conn.state, ConnectionState.CONNECTED)
        self.assertIsNotNone(conn._protocol.schema)
        self.assertIsNotNone(conn.version)

        await conn.call('box.info')
        await conn.disconnect()

    async def test__connect_direct(self):
        conn = await asynctnt.connect(host=self.tnt.host, port=self.tnt.port,
                                      reconnect_timeout=0)
        self.assertEqual(conn.host, self.tnt.host)
        self.assertEqual(conn.port, self.tnt.port)
        self.assertIsNone(conn.username)
        self.assertIsNone(conn.password)
        self.assertEqual(conn.reconnect_timeout, 0)
        self.assertEqual(conn.connect_timeout, 3)
        self.assertEqual(conn.loop, self.loop)
        self.assertIsNone(conn.initial_read_buffer_size)

        self.assertIsNotNone(conn._transport)
        self.assertIsNotNone(conn._protocol)
        self.assertTrue(conn.is_connected)
        self.assertTrue(conn.is_fully_connected)
        self.assertEqual(conn.state, ConnectionState.CONNECTED)
        self.assertIsNotNone(conn._protocol.schema)
        self.assertIsNotNone(conn.version)

        await conn.call('box.info')
        await conn.disconnect()

    async def test__connect_unix(self):
        if self.in_docker:
            self.skipTest('Skipping as running inside the docker')
            return

        tnt = TarantoolSyncInstance(
            host='unix/',
            port='/tmp/' + uuid.uuid4().hex + '.sock',
            console_host='127.0.0.1',
            applua=self.read_applua(),
            cleanup=self.TNT_CLEANUP
        )
        tnt.start()
        try:
            conn = await asynctnt.connect(host=tnt.host, port=tnt.port,
                                          reconnect_timeout=0)
            self.assertEqual(conn.host, tnt.host)
            self.assertEqual(conn.port, tnt.port)
            self.assertIsNone(conn.username)
            self.assertIsNone(conn.password)
            self.assertEqual(conn.reconnect_timeout, 0)
            self.assertEqual(conn.connect_timeout, 3)
            self.assertEqual(conn.loop, self.loop)
            self.assertIsNone(conn.initial_read_buffer_size)

            self.assertIsNotNone(conn._transport)
            self.assertIsNotNone(conn._protocol)
            self.assertTrue(conn.is_connected)
            self.assertTrue(conn.is_fully_connected)
            self.assertEqual(conn.state, ConnectionState.CONNECTED)
            self.assertIsNotNone(conn._protocol.schema)
            self.assertIsNotNone(conn.version)

            await conn.call('box.info')
            await conn.disconnect()
        finally:
            tnt.stop()

    async def test__connect_contextmanager(self):
        conn = asynctnt.Connection(host=self.tnt.host, port=self.tnt.port,
                                   reconnect_timeout=0)
        self.assertEqual(conn.state, ConnectionState.DISCONNECTED)

        async with conn:
            self.assertIsNotNone(conn._transport)
            self.assertIsNotNone(conn._protocol)
            self.assertTrue(conn.is_connected)
            self.assertTrue(conn.is_fully_connected)
            self.assertEqual(conn.state, ConnectionState.CONNECTED)
            self.assertIsNotNone(conn._protocol.schema)
            self.assertIsNotNone(conn.version)

            await conn.call('box.info')

        self.assertEqual(conn.state, ConnectionState.DISCONNECTED)

    async def test__connect_contextmanager_connect_inside(self):
        conn = asynctnt.Connection(host=self.tnt.host, port=self.tnt.port,
                                   reconnect_timeout=0)
        self.assertEqual(conn.state, ConnectionState.DISCONNECTED)

        async with conn:
            await conn.connect()
            self.assertEqual(conn.state, ConnectionState.CONNECTED)
            await conn.call('box.info')

        self.assertEqual(conn.state, ConnectionState.DISCONNECTED)

    async def test__connect_contextmanager_disconnect_inside(self):
        conn = asynctnt.Connection(host=self.tnt.host, port=self.tnt.port,
                                   reconnect_timeout=0)
        self.assertEqual(conn.state, ConnectionState.DISCONNECTED)

        async with conn:
            await conn.disconnect()
            self.assertEqual(conn.state, ConnectionState.DISCONNECTED)

            with self.assertRaises(TarantoolNotConnectedError):
                await conn.call('box.info')

        self.assertEqual(conn.state, ConnectionState.DISCONNECTED)

    async def test__connect_no_schema(self):
        conn = asynctnt.Connection(host=self.tnt.host, port=self.tnt.port,
                                   reconnect_timeout=0,
                                   fetch_schema=False,
                                   auto_refetch_schema=False)
        async with conn:
            self.assertIsNotNone(conn._transport)
            self.assertIsNotNone(conn._protocol)
            self.assertTrue(conn.is_connected)
            self.assertTrue(conn.is_fully_connected)
            self.assertEqual(conn.state, ConnectionState.CONNECTED)
            self.assertIsNotNone(conn._protocol.schema)
            await conn.call('box.info')

    async def test__connect_auth(self):
        conn = asynctnt.Connection(host=self.tnt.host, port=self.tnt.port,
                                   username='t1', password='t1',
                                   reconnect_timeout=0)
        async with conn:
            self.assertIsNotNone(conn._transport)
            self.assertIsNotNone(conn._protocol)
            self.assertTrue(conn.is_connected)
            self.assertTrue(conn.is_fully_connected)
            self.assertEqual(conn.state, ConnectionState.CONNECTED)
            self.assertIsNotNone(conn._protocol.schema)
            await conn.call('box.info')

    async def test__connect_auth_no_schema(self):
        conn = asynctnt.Connection(host=self.tnt.host, port=self.tnt.port,
                                   username='t1', password='t1',
                                   fetch_schema=False,
                                   auto_refetch_schema=False,
                                   reconnect_timeout=0)
        self.assertEqual(conn.username, 't1')
        self.assertEqual(conn.password, 't1')
        async with conn:
            self.assertIsNotNone(conn._transport)
            self.assertIsNotNone(conn._protocol)
            self.assertTrue(conn.is_connected)
            self.assertTrue(conn.is_fully_connected)
            self.assertEqual(conn.state, ConnectionState.CONNECTED)
            self.assertIsNotNone(conn._protocol.schema)
            await conn.call('box.info')

    async def test__disconnect(self):
        conn = asynctnt.Connection(host=self.tnt.host, port=self.tnt.port,
                                   reconnect_timeout=0)
        await conn.connect()
        await conn.disconnect()
        self.assertFalse(conn.is_connected)
        self.assertFalse(conn.is_fully_connected)
        self.assertEqual(conn.state, ConnectionState.DISCONNECTED)

        with self.assertRaises(TarantoolNotConnectedError):
            await conn.call('box.info')

    async def test__disconnect_in_request(self):
        conn = asynctnt.Connection(host=self.tnt.host, port=self.tnt.port,
                                   reconnect_timeout=0)
        await conn.connect()

        coro = self.ensure_future(conn.eval('require "fiber".sleep(2)'))
        await self.sleep(0.5)
        await conn.disconnect()

        with self.assertRaises(TarantoolNotConnectedError):
            await coro

    async def test__disconnect_auth(self):
        conn = asynctnt.Connection(host=self.tnt.host, port=self.tnt.port,
                                   username='t1', password='t1',
                                   reconnect_timeout=0)
        await conn.connect()
        await conn.disconnect()
        self.assertFalse(conn.is_connected)
        self.assertFalse(conn.is_fully_connected)
        self.assertEqual(conn.state, ConnectionState.DISCONNECTED)

        with self.assertRaises(TarantoolNotConnectedError):
            await conn.call('box.info')

    async def test__disconnect_while_reconnecting(self):
        conn = asynctnt.Connection(host=self.tnt.host, port=self.tnt.port,
                                   username='t1', password='t1',
                                   reconnect_timeout=0.1)
        self.assertEqual(conn.reconnect_timeout, 0.1)
        try:
            await conn.connect()
            self.tnt.stop()
            await self.sleep(0.5)

            await conn.disconnect()

            self.assertFalse(conn.is_connected)
            self.assertFalse(conn.is_fully_connected)
            self.assertEqual(conn.state, ConnectionState.DISCONNECTED)

            with self.assertRaises(TarantoolNotConnectedError):
                await conn.call('box.info')
        finally:
            self.tnt.start()

    async def test__close_while_reconnecting(self):
        conn = asynctnt.Connection(host=self.tnt.host, port=self.tnt.port,
                                   username='t1', password='t1',
                                   reconnect_timeout=0.1)
        self.assertEqual(conn.reconnect_timeout, 0.1)
        try:
            await conn.connect()
            self.tnt.stop()
            await self.sleep(0.5)

            conn.close()

            self.assertFalse(conn.is_connected)
            self.assertFalse(conn.is_fully_connected)
            self.assertEqual(conn.state, ConnectionState.DISCONNECTED)

            with self.assertRaises(TarantoolNotConnectedError):
                await conn.call('box.info')
        finally:
            self.tnt.start()

    async def test__connect_multiple(self):
        conn = asynctnt.Connection(host=self.tnt.host, port=self.tnt.port,
                                   fetch_schema=False,
                                   reconnect_timeout=0)
        for _ in range(10):
            await conn.connect()
            await conn.disconnect()
        self.assertFalse(conn.is_connected)
        self.assertFalse(conn.is_fully_connected)
        self.assertEqual(conn.state, ConnectionState.DISCONNECTED)

        with self.assertRaises(TarantoolNotConnectedError):
            await conn.call('box.info')

    async def test__connect_cancel(self):
        conn = asynctnt.Connection(host=self.tnt.host, port=self.tnt.port,
                                   fetch_schema=True,
                                   reconnect_timeout=0)
        try:
            f = asyncio.ensure_future(conn.connect())
            await self.sleep(0.0001)
            f.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await f
        finally:
            await conn.disconnect()

    async def test__connect_error_no_reconnect(self):
        conn = asynctnt.Connection(host="127.0.0.1", port=1,
                                   fetch_schema=True,
                                   reconnect_timeout=0)
        with self.assertRaises(ConnectionRefusedError):
            await conn.connect()

    async def test__connect_wait_tnt_started(self):
        self.tnt.stop()
        conn = asynctnt.Connection(host=self.tnt.host, port=self.tnt.port,
                                   username='t1', password='t1',
                                   fetch_schema=True,
                                   reconnect_timeout=0.000000001)
        try:
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

            await conn.call('box.info')
        finally:
            await conn.disconnect()

    async def test__connect_waiting_for_spaces(self):
        if self.in_docker:
            self.skipTest('not running in docker')
            return

        with self.make_instance() as tnt:
            tnt.replication_source = ['x:1']
            tnt.start(wait=False)

            conn = asynctnt.Connection(host=tnt.host, port=tnt.port,
                                       fetch_schema=True,
                                       reconnect_timeout=0.1,
                                       connect_timeout=10)
            self.assertEqual(conn.connect_timeout, 10)
            try:
                states = {}

                async def state_checker():
                    while True:
                        states[conn.state] = True
                        await self.sleep(0.001)

                checker = self.ensure_future(state_checker())

                try:
                    await asyncio.wait_for(conn.connect(), 1)
                except asyncio.TimeoutError:
                    self.assertTrue(True, 'connect cancelled')

                checker.cancel()

                self.assertTrue(states.get(ConnectionState.CONNECTING, False),
                                'was in connecting')

                with self.assertRaises(TarantoolNotConnectedError):
                    await conn.call('box.info')
            finally:
                await conn.disconnect()

    async def test__connect_waiting_for_spaces_no_reconnect(self):
        if self.in_docker:
            self.skipTest('not running in docker')
            return

        with self.make_instance() as tnt:
            tnt.replication_source = ['x:1']
            tnt.start(wait=False)
            await self.sleep(1)

            if not check_version(self, tnt.version(), min=(1, 7)):
                return

            conn = asynctnt.Connection(host=tnt.host, port=tnt.port,
                                       fetch_schema=True,
                                       reconnect_timeout=0,
                                       connect_timeout=10)
            try:
                with self.assertRaises(TarantoolDatabaseError) as e:
                    await conn.connect()

                self.assertEqual(e.exception.code, ErrorCode.ER_NO_SUCH_SPACE)
            finally:
                await conn.disconnect()

    async def test__connect_waiting_for_spaces_no_reconnect_1_6(self):
        with self.make_instance() as tnt:
            tnt.replication_source = ['x:1']
            tnt.start(wait=False)
            await self.sleep(1)

            if not check_version(self, tnt.version(), max=(1, 7)):
                return

            conn = asynctnt.Connection(host=tnt.host, port=tnt.port,
                                       fetch_schema=True,
                                       reconnect_timeout=0,
                                       connect_timeout=10)
            try:
                with self.assertRaises(ConnectionRefusedError):
                    await conn.connect()
            finally:
                await conn.disconnect()

    async def test__connect_err_loading(self):
        if self.in_docker:
            self.skipTest('not running in docker')
            return

        with self.make_instance() as tnt:
            tnt.replication_source = ['x:1']
            tnt.start(wait=False)
            await self.sleep(1)

            if not check_version(self, tnt.version(), min=(1, 7)):
                return

            conn = asynctnt.Connection(host=tnt.host, port=tnt.port,
                                       username='t1', password='t1',
                                       fetch_schema=True,
                                       reconnect_timeout=0,
                                       connect_timeout=10)
            try:
                with self.assertRaises(TarantoolDatabaseError) as e:
                    await conn.connect()

                self.assertEqual(e.exception.code, ErrorCode.ER_LOADING)
            finally:
                await conn.disconnect()

    async def test__connect_err_loading_1_6(self):
        with self.make_instance() as tnt:
            tnt.replication_source = ['x:1']
            tnt.start(wait=False)

            await self.sleep(1)
            if not check_version(self, tnt.version(), max=(1, 7)):
                return

            conn = asynctnt.Connection(host=tnt.host, port=tnt.port,
                                       username='t1', password='t1',
                                       fetch_schema=True,
                                       reconnect_timeout=0,
                                       connect_timeout=10)
            try:
                with self.assertRaises(ConnectionRefusedError):
                    await conn.connect()
            finally:
                await conn.disconnect()

    async def test__connect_tnt_restarted(self):
        conn = asynctnt.Connection(host=self.tnt.host, port=self.tnt.port,
                                   username='t1', password='t1',
                                   fetch_schema=True,
                                   reconnect_timeout=0.000001)
        await conn.connect()

        try:
            self.tnt.stop()
            self.tnt.start()
            await self.sleep(0.5)
            await conn.ping()
        except Exception as e:
            self.fail(
                'Should not throw any exceptions, but got: {}'.format(e))
        finally:
            await conn.disconnect()

    async def test__connect_force_disconnect(self):
        conn = asynctnt.Connection(host=self.tnt.host, port=44444,
                                   reconnect_timeout=0.3)
        self.ensure_future(conn.connect())
        await self.sleep(1)
        await conn.disconnect()
        self.assertEqual(conn.state, ConnectionState.DISCONNECTED)

    async def test__close(self):
        conn = asynctnt.Connection(host=self.tnt.host, port=self.tnt.port,
                                   reconnect_timeout=0)
        await conn.connect()
        await self.sleep(0.1)
        conn.close()
        await self.sleep(0.1)
        self.assertEqual(conn.state, ConnectionState.DISCONNECTED)

    async def test_disconnect_from_idle(self):
        conn = asynctnt.Connection(host=self.tnt.host, port=self.tnt.port,
                                   reconnect_timeout=0)
        await conn.disconnect()
        self.assertEqual(conn.state, ConnectionState.DISCONNECTED)

    async def test_reconnect_from_idle(self):
        conn = asynctnt.Connection(host=self.tnt.host, port=self.tnt.port,
                                   reconnect_timeout=0)
        await conn.reconnect()
        try:

            self.assertEqual(conn.state, ConnectionState.CONNECTED)
            await conn.call('box.info')
        finally:
            await conn.disconnect()

    async def test_reconnect_after_connect(self):
        conn = asynctnt.Connection(host=self.tnt.host, port=self.tnt.port,
                                   reconnect_timeout=0)
        try:
            await conn.connect()
            await conn.reconnect()

            self.assertEqual(conn.state, ConnectionState.CONNECTED)
            await conn.call('box.info')
        finally:
            await conn.disconnect()

    async def test_manual_reconnect(self):
        conn = asynctnt.Connection(host=self.tnt.host, port=self.tnt.port,
                                   reconnect_timeout=0)
        try:
            await conn.connect()
            await conn.disconnect()
            await conn.connect()

            self.assertEqual(conn.state, ConnectionState.CONNECTED)
            await conn.call('box.info')
        finally:
            await conn.disconnect()

    async def test__connect_connection_lost(self):
        conn = asynctnt.Connection(host=self.tnt.host, port=self.tnt.port,
                                   reconnect_timeout=1/3)
        try:
            await conn.connect()
            self.tnt.stop()
            await self.sleep(0.5)
            self.tnt.start()
            await self.sleep(0.5)

            self.assertEqual(conn.state, ConnectionState.CONNECTED)
            self.assertTrue(conn.is_connected)
            await conn.call('box.info')
        finally:
            await conn.disconnect()

    async def test__connect_from_multiple_coroutines(self):
        conn = asynctnt.Connection(host=self.tnt.host, port=self.tnt.port,
                                   reconnect_timeout=1/3)
        try:
            coros = []
            for _ in range(10):
                coros.append(asyncio.ensure_future(conn.connect()))

            await asyncio.gather(*coros)
            self.assertEqual(conn.state, ConnectionState.CONNECTED)
            self.assertTrue(conn.is_connected)
            await conn.call('box.info')
        finally:
            await conn.disconnect()

    async def test__disconnect_from_multiple_coroutines(self):
        conn = asynctnt.Connection(host=self.tnt.host, port=self.tnt.port,
                                   reconnect_timeout=1/3)
        try:
            await conn.connect()
            coros = []
            for _ in range(10):
                coros.append(asyncio.ensure_future(conn.disconnect()))

            await asyncio.gather(*coros)
            self.assertEqual(conn.state, ConnectionState.DISCONNECTED)
            self.assertFalse(conn.is_connected)

            with self.assertRaises(TarantoolNotConnectedError):
                await conn.call('box.info')
        finally:
            await conn.disconnect()

    async def test__connect_while_reconnecting(self):
        conn = asynctnt.Connection(host=self.tnt.host, port=self.tnt.port,
                                   reconnect_timeout=1)

        try:
            coros = []
            for _ in range(10):
                coros.append(asyncio.ensure_future(conn.connect()))

            self.tnt.stop()
            await self.sleep(0.5)

            connect_coros = asyncio.ensure_future(asyncio.gather(*coros))

            self.tnt.start()
            await self.sleep(1)
            await connect_coros

            self.assertEqual(conn.state, ConnectionState.CONNECTED)
            self.assertTrue(conn.is_connected)

            await conn.call('box.info')
        finally:
            await conn.disconnect()

    async def test__connect_on_tnt_crash_no_reconnect(self):
        conn = asynctnt.Connection(host=self.tnt.host, port=self.tnt.port,
                                   connect_timeout=1,
                                   reconnect_timeout=0)
        try:
            await conn.connect()
            await asyncio.sleep(0.5)
            try:
                await conn.eval("require('ffi').cast('char *', 0)[0] = 48")
            except TarantoolNotConnectedError:
                self.assertTrue(True, 'not connected error triggered')
            self.tnt.stop()
            self.tnt.start()
            await asyncio.sleep(1)
            await conn.connect()  # this connect should reconnect easily

            await conn.call('box.info')
        finally:
            await conn.disconnect()

    async def test__connect_on_tnt_crash_with_reconnect(self):
        conn = asynctnt.Connection(host=self.tnt.host, port=self.tnt.port,
                                   connect_timeout=1,
                                   reconnect_timeout=1/3)
        try:
            await conn.connect()
            await asyncio.sleep(0.5)
            try:
                await conn.eval("require('ffi').cast('char *', 0)[0] = 48")
            except TarantoolNotConnectedError:
                self.assertTrue(True, 'not connected error triggered')
            self.tnt.stop()
            self.tnt.start()
            await asyncio.sleep(1)
            await conn.connect()  # this connect should reconnect easily

            await conn.call('box.info')
        finally:
            await conn.disconnect()

    async def test__connect_invalid_user_no_reconnect(self):
        conn = asynctnt.Connection(host=self.tnt.host, port=self.tnt.port,
                                   username='fancy', password='man',
                                   connect_timeout=1,
                                   reconnect_timeout=0)
        with self.assertRaises(TarantoolDatabaseError) as e:
            await conn.connect()

        self.assertEqual(e.exception.code, ErrorCode.ER_NO_SUCH_USER)

    async def test__connect_invalid_user_with_reconnect(self):
        conn = asynctnt.Connection(host=self.tnt.host, port=self.tnt.port,
                                   fetch_schema=True,
                                   reconnect_timeout=0.1,
                                   connect_timeout=10)
        await conn.connect()  # first connect successfully

        # then change credentials
        conn._username = 'fancy'
        conn._password = 'man'

        self.tnt.stop()
        self.tnt.start()
        await self.sleep(0.1)
        try:
            states = {}

            async def state_checker():
                while True:
                    states[conn.state] = True
                    await self.sleep(0.001)

            checker = self.ensure_future(state_checker())

            try:
                await asyncio.wait_for(conn.connect(), 1)
            except asyncio.TimeoutError:
                self.assertTrue(True, 'connect cancelled')

            checker.cancel()

            self.assertTrue(states.get(ConnectionState.CONNECTING, False),
                            'was in connecting')
            self.assertTrue(states.get(ConnectionState.RECONNECTING, False),
                            'was in connecting')

            with self.assertRaises(TarantoolNotConnectedError):
                await conn.call('box.info')
        finally:
            await conn.disconnect()
