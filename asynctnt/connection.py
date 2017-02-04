import asyncio
import functools
import os

from asynctnt.iproto import protocol
from asynctnt.log import logger


__all__ = (
    'Connection', 'connect'
)


class Connection:
    __slots__ = (
        '_host', '_port', '_username', '_password', '_fetch_schema',
        '_encoding', '_connect_timeout', '_reconnect_timeout',
        '_request_timeout', '_loop',
        '_transport', '_protocol', '_closing', '_disconnect_waiter'
    )

    def __init__(self, *,
                 host=None,
                 port=None,
                 username=None,
                 password=None,
                 fetch_schema=True,
                 connect_timeout=60,
                 request_timeout=None,
                 reconnect_timeout=1. / 3.,
                 encoding='utf-8',
                 loop=None):

        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._fetch_schema = fetch_schema
        self._encoding = encoding

        self._connect_timeout = connect_timeout
        self._reconnect_timeout = reconnect_timeout or 0
        self._request_timeout = request_timeout

        self._loop = loop or asyncio.get_event_loop()

        self._transport = None
        self._protocol = None

        self._closing = False
        self._disconnect_waiter = None

    def connection_lost(self, exc):
        if self._reconnect_timeout > 0 and not self._closing:
            logger.info('Tarantool[%s:%s] Starting reconnecting',
                        self._host, self._port)
            asyncio.ensure_future(self.connect(), loop=self._loop)
        else:
            self._closing = False
            if self._disconnect_waiter:
                self._disconnect_waiter.set_result(True)
                self._disconnect_waiter = None

    def protocol_factory(self, connected_fut, cls=protocol.Protocol):
        return cls(host=self._host,
                   port=self._port,
                   username=self._username,
                   password=self._password,
                   fetch_schema=self._fetch_schema,
                   request_timeout=self._request_timeout,
                   encoding=self._encoding,
                   connected_fut=connected_fut,
                   on_connection_lost=self.connection_lost,
                   loop=self._loop)

    async def connect(self):
        if self.is_connected:
            return

        is_unix = self._host.startswith('unix/')

        connected_fut = _create_future(self._loop)

        while True:
            try:
                if is_unix:
                    unix_path = self._port
                    assert unix_path, \
                        'No unix file path specified'
                    assert os.path.isfile(unix_path), \
                        'Unix socket `{}` not found'.format(unix_path)

                    conn = self._loop.create_unix_connection(
                        functools.partial(self.protocol_factory,
                                          connected_fut),
                        unix_path
                    )
                else:
                    conn = self._loop.create_connection(
                        functools.partial(self.protocol_factory,
                                          connected_fut),
                        self._host, self._port)

                try:
                    tr, pr = await asyncio.wait_for(
                        conn, timeout=self._connect_timeout, loop=self._loop)
                except (OSError, asyncio.TimeoutError):
                    raise

                logger.info('Tarantool[%s:%s] Connected successfully',
                            self._host, self._port)

                try:
                    await connected_fut
                except:
                    tr.close()
                    raise

                self._transport = tr
                self._protocol = pr
                return
            except (OSError, asyncio.TimeoutError) as e:
                if self._reconnect_timeout > 0:
                    logger.warning(
                        'Connecting to Tarantool[%s:%s] failed. '
                        'Retrying in %i seconds',
                        self._host, self._port, self._reconnect_timeout)

                    await asyncio.sleep(self._reconnect_timeout,
                                        loop=self._loop)
                else:
                    raise

    def disconnect(self):
        logger.info('Disconnecting from Tarantool[{}:{}]'.format(self._host,
                                                                 self._port))
        self._closing = True
        waiter = _create_future(self._loop)
        if self._transport:
            self._disconnect_waiter = waiter
            self._transport.close()
        else:
            waiter.set_result(True)
        return waiter

    async def reconnect(self):
        await self.disconnect()
        await self.connect()

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    @property
    def username(self):
        return self._username

    @property
    def password(self):
        return self._password

    @property
    def fetch_schema(self):
        return self._fetch_schema

    @property
    def encoding(self):
        return self._encoding

    @property
    def reconnect_timeout(self):
        return self._reconnect_timeout

    @property
    def connect_timeout(self):
        return self._connect_timeout

    @property
    def request_timeout(self):
        return self._request_timeout

    @property
    def version(self):
        if self._protocol is None:
            return None
        return self._protocol.version

    @property
    def loop(self):
        return self._loop

    @property
    def is_connected(self):
        if self._protocol is None:
            return False
        return self._protocol.is_connected()

    @property
    def schema(self):
        if self._protocol is None:
            return None
        return self._protocol.schema

    def refetch_schema(self):
        return self._protocol.refetch_schema()

    def ping(self, *, timeout=0):
        return self._protocol.ping(timeout=timeout)

    def auth(self, username, password, *, timeout=0):
        return self._protocol.auth(username, password,
                                   timeout=timeout)

    def call16(self, func_name, args=None, *, timeout=0):
        return self._protocol.call16(func_name, args,
                                     timeout=timeout)

    def call(self, func_name, args=None, *, timeout=0):
        return self._protocol.call(func_name, args,
                                   timeout=timeout)

    def eval(self, expression, args=None, *, timeout=0):
        return self._protocol.eval(expression, args,
                                   timeout=timeout)

    def select(self, space, key=None, **kwargs):
        return self._protocol.select(space, key, **kwargs)

    def insert(self, space, t, *, replace=False, timeout=0):
        return self._protocol.insert(space, t,
                                     replace=replace, timeout=timeout)

    def replace(self, space, t, *, timeout=0):
        return self._protocol.replace(space, t,
                                      timeout=timeout)

    def delete(self, space, key, **kwargs):
        return self._protocol.delete(space, key, **kwargs)

    def update(self, space, key, operations, **kwargs):
        return self._protocol.update(space, key, operations, **kwargs)

    def upsert(self, space, t, operations, **kwargs):
        return self._protocol.upsert(space, t, operations, **kwargs)


def _create_future(loop):
    try:
        return loop.create_future()
    except AttributeError:
        return asyncio.Future(loop=loop)


async def connect(**kwargs):
    c = Connection(**kwargs)
    await c.connect()
    return c
