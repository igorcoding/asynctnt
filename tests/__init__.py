import logging
import os
import sys

from asynctnt._testbase import TarantoolTestCase

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))


class BaseTarantoolTestCase(TarantoolTestCase):
    DO_CONNECT = True
    LOGGING_LEVEL = logging.CRITICAL
    LOGGING_STREAM = sys.stdout
    TNT_APP_LUA_PATH = os.path.join(CURRENT_DIR, 'files', 'app.lua')

    TESTER_SPACE_ID = 512
    TESTER_SPACE_NAME = 'tester'

    async def truncate(self):
        if self.conn.is_connected:
            await self.conn.call('truncate', timeout=5)

    def tearDown(self):
        if hasattr(self, 'conn'):
            self.loop.run_until_complete(self.truncate())
        super().tearDown()
