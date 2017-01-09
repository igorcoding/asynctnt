import asyncio

from tests import MyBaseTarantoolTestCase

import asynctnt


class ConnectTestCase(MyBaseTarantoolTestCase):
    DO_CONNECT = False
    
    async def test_connect(self):
        conn = asynctnt.Connection(host=self.tnt.host, port=self.tnt.port,
                                   reconnect_timeout=0,
                                   loop=self.loop)
        await conn.connect()
        self.assertIsNotNone(conn._transport)
        self.assertIsNotNone(conn._protocol)
        self.assertTrue(conn.is_connected)
        self.assertTrue(conn._protocol.is_fully_connected())
        self.assertIsNotNone(conn._protocol.schema)
        await conn.disconnect()
    
    async def test_connect_no_schema(self):
        conn = asynctnt.Connection(host=self.tnt.host, port=self.tnt.port,
                                   reconnect_timeout=0, fetch_schema=False,
                                   loop=self.loop)
        await conn.connect()
        self.assertIsNotNone(conn._transport)
        self.assertIsNotNone(conn._protocol)
        self.assertTrue(conn.is_connected)
        self.assertTrue(conn._protocol.is_fully_connected())
        self.assertIsNone(conn._protocol.schema)
        await conn.disconnect()
    
    async def test_connect_auth(self):
        conn = asynctnt.Connection(host=self.tnt.host, port=self.tnt.port,
                                   username='t1', password='t1',
                                   reconnect_timeout=0,
                                   loop=self.loop)
        await conn.connect()
        self.assertIsNotNone(conn._transport)
        self.assertIsNotNone(conn._protocol)
        self.assertTrue(conn.is_connected)
        self.assertTrue(conn._protocol.is_fully_connected())
        self.assertIsNotNone(conn._protocol.schema)
        await conn.disconnect()
    
    async def test_connect_auth_no_schema(self):
        conn = asynctnt.Connection(host=self.tnt.host, port=self.tnt.port,
                                   username='t1', password='t1',
                                   fetch_schema=False, reconnect_timeout=0,
                                   loop=self.loop)
        await conn.connect()
        self.assertIsNotNone(conn._transport)
        self.assertIsNotNone(conn._protocol)
        self.assertTrue(conn.is_connected)
        self.assertTrue(conn._protocol.is_fully_connected())
        self.assertIsNone(conn._protocol.schema)
        await conn.disconnect()
    
    async def test_disconnect(self):
        conn = asynctnt.Connection(host=self.tnt.host, port=self.tnt.port,
                                   reconnect_timeout=0,
                                   loop=self.loop)
        await conn.connect()
        await conn.disconnect()
        self.assertFalse(conn.is_connected)
        self.assertFalse(conn._protocol.is_fully_connected())
        self.assertIsNone(conn._protocol.schema)
    
    async def test_disconnect_auth(self):
        conn = asynctnt.Connection(host=self.tnt.host, port=self.tnt.port,
                                   username='t1', password='t1',
                                   reconnect_timeout=0,
                                   loop=self.loop)
        await conn.connect()
        await conn.disconnect()
        self.assertFalse(conn.is_connected)
        self.assertFalse(conn._protocol.is_fully_connected())
        self.assertIsNone(conn._protocol.schema)
    
    async def test_connect_multiple(self):
        conn = asynctnt.Connection(host=self.tnt.host, port=self.tnt.port,
                                   reconnect_timeout=0,
                                   loop=self.loop)
        for _ in range(100):
            await conn.connect()
            await conn.disconnect()
        self.assertFalse(conn.is_connected)
        self.assertFalse(conn._protocol.is_fully_connected())
    
    async def test_connect_cancel(self):
        conn = asynctnt.Connection(host=self.tnt.host, port=self.tnt.port,
                                   reconnect_timeout=0,
                                   loop=self.loop)
        f = asyncio.ensure_future(conn.connect(), loop=self.loop)
        f.cancel()
        try:
            await f
        except asyncio.CancelledError:
            self.assertTrue(True)
        
        await conn.disconnect()
