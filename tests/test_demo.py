import pyximport; pyximport.install()

import asynctnt
from asynctnt._testbase import TarantoolTestCase


class ConnectTestCase(TarantoolTestCase):
    async def test_connect(self):
        conn = asynctnt.Connection(host=self.tnt.host, port=self.tnt.port, loop=self.loop)
        await conn.connect()
        print(await conn.ping())
        await conn.disconnect()
