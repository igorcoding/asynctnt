import logging
import os

import pyximport; pyximport.install()
from asynctnt._testbase import TarantoolTestCase


CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))


class MyBaseTarantoolTestCase(TarantoolTestCase):
    DO_CONNECT = True
    LOGGING_LEVEL = logging.ERROR
    TNT_APP_LUA_PATH = os.path.join(CURRENT_DIR, 'files', 'app.lua')
