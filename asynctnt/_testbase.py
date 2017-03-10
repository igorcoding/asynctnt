import asyncio
import contextlib
import functools
import inspect
import logging
import os
import time
import unittest

import sys

import atexit

import asynctnt
from asynctnt.instance import \
    TarantoolSyncInstance, TarantoolSyncDockerInstance

__all__ = (
    'TestCase', 'TarantoolTestCase'
)


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

    @classmethod
    def ensure_future(cls, coro_or_future):
        return asyncio.ensure_future(coro_or_future, loop=cls.loop)

    @classmethod
    def sleep(cls, delay, result=None):
        return asyncio.sleep(delay, result, loop=cls.loop)


class TarantoolTestCase(TestCase):
    DO_CONNECT = True
    LOGGING_LEVEL = logging.WARNING
    LOGGING_STREAM = sys.stderr
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
        logging.basicConfig(level=cls.LOGGING_LEVEL,
                            stream=cls.LOGGING_STREAM)
        tarantool_docker_version = os.getenv('TARANTOOL_DOCKER_VERSION')
        if tarantool_docker_version:
            print('Running tarantool in docker. Version = {}'.format(
                tarantool_docker_version))
            tnt = TarantoolSyncDockerInstance(
                applua=cls.read_applua(),
                version=tarantool_docker_version
            )
        else:
            unix_path = os.getenv('TARANTOOL_LISTEN_UNIX_PATH')
            if not unix_path:
                tnt = TarantoolSyncInstance(
                    port=TarantoolSyncInstance.get_random_port(),
                    console_port=TarantoolSyncInstance.get_random_port(),
                    applua=cls.read_applua(),
                    cleanup=cls.TNT_CLEANUP
                )
            else:
                tnt = TarantoolSyncInstance(
                    host='unix/',
                    port=unix_path,
                    console_host='127.0.0.1',
                    applua=cls.read_applua(),
                    cleanup=cls.TNT_CLEANUP
                )
        tnt.start()
        cls.tnt = tnt

    @classmethod
    def tearDownClass(cls):
        if cls.tnt:
            cls.tnt.stop()
        TestCase.tearDownClass()

    def setUp(self):
        super(TarantoolTestCase, self).setUp()
        if self.DO_CONNECT:
            self.loop.run_until_complete(self.tnt_connect())

    def tearDown(self):
        self.loop.run_until_complete(self.tnt_disconnect())
        super(TarantoolTestCase, self).tearDown()

    async def tnt_connect(self, *,
                          username=None, password=None,
                          fetch_schema=True,
                          auto_refetch_schema=False,
                          connect_timeout=None, reconnect_timeout=1/3,
                          request_timeout=None, encoding='utf-8',
                          tuple_as_dict=False,
                          initial_read_buffer_size=None):
        self.conn = asynctnt.Connection(
            host=self.tnt.host,
            port=self.tnt.port,
            username=username,
            password=password,
            fetch_schema=fetch_schema,
            auto_refetch_schema=auto_refetch_schema,
            connect_timeout=connect_timeout,
            reconnect_timeout=reconnect_timeout,
            request_timeout=request_timeout,
            encoding=encoding,
            tuple_as_dict=tuple_as_dict,
            initial_read_buffer_size=initial_read_buffer_size,
            loop=self.loop)
        await self.conn.connect()
        return self.conn

    async def tnt_disconnect(self):
        if hasattr(self, 'conn') and self.conn is not None:
            await self.conn.disconnect()
            self.conn = None

    async def tnt_reconnect(self, **kwargs):
        await self.tnt_disconnect()
        await self.tnt_connect(**kwargs)

