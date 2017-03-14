import asyncio
import os

import sys

import logging

from asynctnt import Response
from asynctnt.exceptions import TarantoolNotConnectedError
from tests import BaseTarantoolTestCase


class PingTestCase(BaseTarantoolTestCase):
    SMALL_TIMEOUT = 0.00000000001
    LOGGING_LEVEL = logging.DEBUG

    async def test__ping_basic(self):
        res = await self.conn.ping()
        self.assertIsNotNone(res)
        self.assertIsInstance(res, Response)
        self.assertGreater(res.sync, 0, 'Sync is not 0')
        self.assertEqual(res.code, 0, 'Code is 0')
        self.assertEqual(res.return_code, 0, 'Return code is 0')
        self.assertIsNone(res.body, 'No body for ping')

    async def test__ping_timeout_on_conn(self):
        await self.tnt_reconnect(request_timeout=self.SMALL_TIMEOUT)

        try:
            await self.conn.ping(timeout=1)
        except:
            self.fail('Should not fail on timeout 1')

    async def test__ping_connection_lost(self):
        self.tnt.stop()
        await self.sleep(0)

        try:
            os.kill(self.tnt.pid, 0)
            running = True
        except:
            running = False

        with self.assertRaises(TarantoolNotConnectedError):
            res = await self.conn.ping()
            print(res)
            print('running', running)
            print(os.system('ps aux | grep tarantool'))

        self.tnt.start()
        await self.sleep(1)

        try:
            await self.conn.ping()
        except:
            self.fail('Should not fail after tnt start')

    async def test__ping_with_reconnect(self):
        await self.conn.reconnect()
        res = await self.conn.ping()
        self.assertIsInstance(res, Response, 'Ping result')
