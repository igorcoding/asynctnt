import asyncio

import logging

from asynctnt.connection import ConnectionState
from tests import BaseTarantoolTestCase

import asynctnt


class ConnectTestCase(BaseTarantoolTestCase):
    DO_CONNECT = False

    async def test__connect(self):
        conn = asynctnt.Connection(host=self.tnt.host, port=self.tnt.port,
                                   reconnect_timeout=0,
                                   loop=self.loop)
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
        self.assertIsNone(conn._protocol.schema)
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
        self.assertIsNone(conn._protocol.schema)
        await conn.disconnect()

    async def test__disconnect(self):
        conn = asynctnt.Connection(host=self.tnt.host, port=self.tnt.port,
                                   reconnect_timeout=0,
                                   loop=self.loop)
        await conn.connect()
        await conn.disconnect()
        self.assertFalse(conn.is_connected)
        self.assertEqual(conn.state, ConnectionState.DISCONNECTED)
        self.assertIsNone(conn.schema)

    async def test__disconnect_auth(self):
        conn = asynctnt.Connection(host=self.tnt.host, port=self.tnt.port,
                                   username='t1', password='t1',
                                   reconnect_timeout=0,
                                   loop=self.loop)
        await conn.connect()
        await conn.disconnect()
        self.assertFalse(conn.is_connected)
        self.assertEqual(conn.state, ConnectionState.DISCONNECTED)
        self.assertIsNone(conn.schema)

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
        await self.tnt.stop()
        conn = asynctnt.Connection(host=self.tnt.host, port=self.tnt.port,
                                   username='t1', password='t1',
                                   fetch_schema=True,
                                   reconnect_timeout=0.000001,
                                   loop=self.loop)
        coro = self.ensure_future(conn.connect())
        await self.sleep(0.3)
        await self.tnt.start()
        await self.sleep(1)
        await coro
        self.assertEqual(conn.state, ConnectionState.CONNECTED)
        await conn.disconnect()

    async def test__connect_tnt_restarted(self):
        conn = asynctnt.Connection(host=self.tnt.host, port=self.tnt.port,
                                   username='t1', password='t1',
                                   fetch_schema=True,
                                   reconnect_timeout=0.000001,
                                   loop=self.loop)
        await conn.connect()

        await self.tnt.stop()
        await self.tnt.start()
        await self.sleep(0.5)
        try:
            await conn.ping()
        except Exception as e:
            self.fail(
                'Should not throw any exceptions, but got: {}'.format(e))
        finally:
            await conn.disconnect()

