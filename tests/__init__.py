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
