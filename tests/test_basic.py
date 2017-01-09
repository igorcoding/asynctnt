import asyncio

from tests import MyBaseTarantoolTestCase

import asynctnt


class BasicTestCase(MyBaseTarantoolTestCase):
    DO_CONNECT = True
    
    async def test_ping(self):
        await self.conn.ping()
        self.assertTrue(True)
