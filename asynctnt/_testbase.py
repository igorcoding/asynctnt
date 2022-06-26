import asyncio
import contextlib
import functools
import inspect
import logging
import os
import time
import unittest

import sys
from typing import Tuple, Optional

import asynctnt
from asynctnt.instance import \
    TarantoolSyncInstance, TarantoolSyncDockerInstance
from asynctnt import TarantoolTuple
from asynctnt.utils import get_running_loop

__all__ = (
    'TestCase', 'TarantoolTestCase', 'ensure_version', 'check_version',
    'ensure_bin_version',
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
    _loop = None
    _use_uvloop = False

    @classmethod
    def setUpClass(cls):
        cls._use_uvloop = os.environ.get('USE_UVLOOP')

        if cls._use_uvloop:
            import uvloop
            uvloop.install()

        try:
            loop = get_running_loop()
            # if there is a running loop - close it
            loop.close()
        except RuntimeError:
            pass

        cls._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(cls._loop)

        if cls._use_uvloop:
            import uvloop
            assert isinstance(cls._loop, uvloop.Loop)

    @classmethod
    def tearDownClass(cls):
        if cls._loop is not None:
            cls._loop.close()
            cls._loop = None

    @property
    def loop(self):
        return self._loop

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
        return asyncio.ensure_future(coro_or_future)

    @classmethod
    def sleep(cls, delay, result=None):
        return asyncio.sleep(delay, result)


class TarantoolTestCase(TestCase):
    DO_CONNECT = True
    LOGGING_LEVEL = logging.WARNING
    LOGGING_STREAM = sys.stderr
    TNT_APP_LUA_PATH = None
    TNT_CLEANUP = True
    EXTRA_BOX_CFG = ""

    tnt = None
    in_docker = False

    @classmethod
    def read_applua(cls):
        if cls.TNT_APP_LUA_PATH:
            with open(cls.TNT_APP_LUA_PATH, 'r') as f:
                return f.read()

    @classmethod
    def init_instance(cls):
        tnt, in_docker = cls._make_instance(
            extra_box_cfg=cls.EXTRA_BOX_CFG
        )
        cls.tnt = tnt
        cls.in_docker = in_docker

    @classmethod
    def setUpClass(cls):
        TestCase.setUpClass()
        logging.basicConfig(level=cls.LOGGING_LEVEL,
                            stream=cls.LOGGING_STREAM)
        if cls.tnt is None:
            tnt, in_docker = cls._make_instance(
                extra_box_cfg=cls.EXTRA_BOX_CFG
            )
            cls.tnt = tnt
            cls.in_docker = in_docker

        cls.tnt.start()

    @classmethod
    def make_instance(cls):
        obj, _ = cls._make_instance()
        return obj

    @classmethod
    def _make_instance(cls, **kwargs):
        tarantool_docker_image = os.getenv('TARANTOOL_DOCKER_IMAGE')
        tarantool_docker_tag = os.getenv('TARANTOOL_DOCKER_VERSION')
        in_docker = False
        if tarantool_docker_tag:
            print('Running tarantool in docker: {}:{}'.format(
                tarantool_docker_image or 'tarantool/tarantool',
                tarantool_docker_tag))

            tnt = TarantoolSyncDockerInstance(
                applua=cls.read_applua(),
                docker_image=tarantool_docker_image,
                docker_tag=tarantool_docker_tag,
                timeout=4 * 60,
                **kwargs
            )
            in_docker = True
        else:
            unix_path = os.getenv('TARANTOOL_LISTEN_UNIX_PATH')
            if not unix_path:
                tnt = TarantoolSyncInstance(
                    port=TarantoolSyncInstance.get_random_port(),
                    console_port=TarantoolSyncInstance.get_random_port(),
                    applua=cls.read_applua(),
                    cleanup=cls.TNT_CLEANUP,
                    **kwargs
                )
            else:
                tnt = TarantoolSyncInstance(
                    host='unix/',
                    port=unix_path,
                    console_host='127.0.0.1',
                    applua=cls.read_applua(),
                    cleanup=cls.TNT_CLEANUP,
                    **kwargs
                )

        return tnt, in_docker

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

    @property
    def conn(self) -> asynctnt.Connection:
        return self._conn

    async def tnt_connect(self, *,
                          username=None, password=None,
                          fetch_schema=True,
                          auto_refetch_schema=False,
                          connect_timeout=None, reconnect_timeout=1 / 3,
                          ping_timeout=0,
                          request_timeout=None, encoding='utf-8',
                          initial_read_buffer_size=None):
        self._conn = asynctnt.Connection(
            host=self.tnt.host,
            port=self.tnt.port,
            username=username,
            password=password,
            fetch_schema=fetch_schema,
            auto_refetch_schema=auto_refetch_schema,
            connect_timeout=connect_timeout,
            reconnect_timeout=reconnect_timeout,
            request_timeout=request_timeout,
            ping_timeout=ping_timeout,
            encoding=encoding,
            initial_read_buffer_size=initial_read_buffer_size)
        await self._conn.connect()
        return self._conn

    async def tnt_disconnect(self):
        if hasattr(self, 'conn') and self.conn is not None:
            await self._conn.disconnect()
            self._conn = None

    async def tnt_reconnect(self, **kwargs):
        await self.tnt_disconnect()
        await self.tnt_connect(**kwargs)

    def assertResponseEqual(self, resp, target, *args):
        tuples = []
        for item in resp:
            if isinstance(item, TarantoolTuple):
                item = list(item)
            tuples.append(item)
        return self.assertListEqual(tuples, target, *args)

    def assertResponseEqualKV(self, resp, target, *args):
        tuples = []
        for item in resp:
            if isinstance(item, TarantoolTuple):
                item = dict(item)
            tuples.append(item)
        return self.assertListEqual(tuples, target, *args)


def ensure_version(*, min=None, max=None,
                   min_included=True, max_included=False):
    def check_version_wrap(f):
        @functools.wraps(f)
        async def wrap(self, *args, **kwargs):
            if check_version(self, self.conn.version,
                             min=min, max=max,
                             min_included=min_included,
                             max_included=max_included):
                res = f(self, *args, **kwargs)
                if inspect.isawaitable(res):
                    return await res
                return res

        return wrap

    return check_version_wrap


def ensure_bin_version(*, min=None, max=None,
                       min_included=True, max_included=False):
    def check_version_wrap(cls):
        assert issubclass(cls, TarantoolTestCase)
        cls.init_instance()
        ok, reason = _check_version(cls.tnt.bin_version,
                                    min=min, max=max,
                                    min_included=min_included,
                                    max_included=max_included)

        if not ok:
            cls = unittest.skip(reason)(cls)

        return cls

    return check_version_wrap


def check_version(test, version, *, min=None, max=None,
                  min_included=False, max_included=False):
    ok, reason = _check_version(version, min=min, max=max,
                                min_included=min_included,
                                max_included=max_included)

    if not ok:
        test.skipTest(reason)
        return False

    return True


def _check_version(version, *, min=None, max=None,
                   min_included=False,
                   max_included=False) -> Tuple[bool, Optional[str]]:
    if min and (version < min or (min_included and version <= min)):
        return False, f'version mismatch - required min={min} got={version}'

    if max and (version > max or (max_included and version >= max)):
        return False, f'version mismatch - required max={max} got={version}'

    return True, None
