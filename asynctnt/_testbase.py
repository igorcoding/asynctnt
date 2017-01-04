import asyncio
import atexit
import contextlib
import functools
import inspect
import logging
import os
import time
import unittest

import asynctnt
from asynctnt.instance import TarantoolInstance


@contextlib.contextmanager
def silence_asyncio_long_exec_warning():
    def flt(log_record):
        msg = log_record.getMessage()
        return not msg.startswith('Executing ')

    logger = logging.getLogger('asyncio')
    logger.addFilter(flt)
    try:
        yield
    finally:
        logger.removeFilter(flt)


class TestCaseMeta(type(unittest.TestCase)):

    @staticmethod
    def _iter_methods(bases, ns):
        for base in bases:
            for methname in dir(base):
                if not methname.startswith('test_'):
                    continue

                meth = getattr(base, methname)
                if not inspect.iscoroutinefunction(meth):
                    continue

                yield methname, meth

        for methname, meth in ns.items():
            if not methname.startswith('test_'):
                continue

            if not inspect.iscoroutinefunction(meth):
                continue

            yield methname, meth

    def __new__(mcls, name, bases, ns):
        for methname, meth in mcls._iter_methods(bases, ns):
            @functools.wraps(meth)
            def wrapper(self, *args, __meth__=meth, **kwargs):
                self.loop.run_until_complete(__meth__(self, *args, **kwargs))
            ns[methname] = wrapper

        return super().__new__(mcls, name, bases, ns)


class TestCase(unittest.TestCase, metaclass=TestCaseMeta):
    loop = None

    @classmethod
    def setUpClass(cls):
        if os.environ.get('USE_UVLOOP'):
            import uvloop
            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)
        asyncio.get_child_watcher().attach_loop(loop)
        cls.loop = loop

    @classmethod
    def tearDownClass(cls):
        if cls.loop:
            cls.loop.close()
        asyncio.set_event_loop(None)

    @contextlib.contextmanager
    def assertRunUnder(self, delta):
        st = time.monotonic()
        try:
            yield
        finally:
            if time.monotonic() - st > delta:
                raise AssertionError(
                    'running block took longer than {}'.format(delta))


class TarantoolTestCase(TestCase):
    DO_CONNECT = True
    LOGGING_LEVEL = logging.WARNING
    TNT_APP_LUA_PATH = None
    TNT_CLEANUP = True

    tnt = None
    
    @classmethod
    def read_applua(cls):
        if cls.TNT_APP_LUA_PATH:
            with open(cls.TNT_APP_LUA_PATH, 'r') as f:
                return f.read()
    
    @classmethod
    def setUpClass(cls):
        TestCase.setUpClass()
        logging.basicConfig(level=cls.LOGGING_LEVEL)
        tnt = TarantoolInstance(
            applua=cls.read_applua(),
            cleanup=cls.TNT_CLEANUP,
            loop=cls.loop
        )
        cls.loop.run_until_complete(tnt.start())
        cls.tnt = tnt
        
    @classmethod
    def tearDownClass(cls):
        if cls.tnt:
            cls.loop.run_until_complete(cls.tnt.stop())
        TestCase.tearDownClass()
        
    def setUp(self):
        super(TarantoolTestCase, self).setUp()
        if self.DO_CONNECT:
            self.conn = asynctnt.Connection(host=self.tnt.host, port=self.tnt.port, loop=self.loop)
            self.loop.run_until_complete(self.conn.connect())
    
    def tearDown(self):
        if hasattr(self, 'conn') and self.conn is not None:
            self.loop.run_until_complete(self.conn.disconnect())
        super(TarantoolTestCase, self).tearDown()
