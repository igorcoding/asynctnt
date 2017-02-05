import asyncio
import enum
import functools
import os

from asynctnt.exceptions import TarantoolDatabaseError, ErrorCode, TarantoolNotConnectedError
from asynctnt.iproto import protocol
from asynctnt.log import logger


__all__ = (
    'Connection', 'connect'
)


class ConnectionState(enum.IntEnum):
    CONNECTING = 0
    CONNECTED = 1
    RECONNECTING = 2
    DISCONNECTING = 3
    DISCONNECTED = 4


class Connection:
    __slots__ = (
        '_host', '_port', '_username', '_password', '_fetch_schema',
        '_encoding', '_connect_timeout', '_reconnect_timeout',
        '_request_timeout', '_loop', '_state', '_state_prev',
        '_transport', '_protocol', '_disconnect_waiter', '_reconnect_coro'
    )

    def __init__(self, *,
                 host='127.0.0.1',
                 port=3301,
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

        self._state = ConnectionState.DISCONNECTED
        self._state_prev = ConnectionState.DISCONNECTED
        self._disconnect_waiter = None
        self._reconnect_coro = None

    def _set_state(self, new_state):
        if self._state != new_state:
            logger.debug(
                'Changing state {} -> {}'.format(repr(self._state),
                                                 repr(new_state)))
            self._state_prev = self._state
            self._state = new_state
            return True
        return False

    def connection_made(self):
        self._set_state(ConnectionState.CONNECTED)

    def connection_lost(self, exc):
        #print('connection_lost: {}'.format(exc))

        if self._transport:
            self._transport.close()
        self._transport = None

        if self._reconnect_timeout > 0 \
                and self._state != ConnectionState.DISCONNECTING:
            self._set_state(ConnectionState.DISCONNECTING)
            self._set_state(ConnectionState.RECONNECTING)
            logger.info('{} Started reconnecting'.format(self.fingerprint))
            self._reconnect_coro = \
                asyncio.ensure_future(self._connect(return_exceptions=False),
                                      loop=self._loop)
        else:
            self._set_state(ConnectionState.DISCONNECTED)
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
                   on_connection_made=self.connection_made,
                   on_connection_lost=self.connection_lost,
                   loop=self._loop)

    async def _connect(self, return_exceptions=True):
        while True:
            try:
                ignore_states = {
                    ConnectionState.CONNECTING,
                    ConnectionState.CONNECTED,
                    ConnectionState.DISCONNECTING,
                }
                if self._state in ignore_states:
                    return

                self._set_state(ConnectionState.CONNECTING)

                #print('__ Started connecting to Tarantool __')
                connected_fut = _create_future(self._loop)

                if self._host.startswith('unix/'):
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

                try:
                    await connected_fut
                except:
                    tr.close()
                    # ignoring reconnect by on_connection_lost
                    self._set_state(ConnectionState.DISCONNECTING)
                    raise

                logger.info(
                    '{} Connected successfully'.format(self.fingerprint))
                self._set_state(ConnectionState.CONNECTED)

                self._transport = tr
                self._protocol = pr
                self._reconnect_coro = None
                return
            except TarantoolDatabaseError as e:
                #print(repr(e))
                if e.code in {ErrorCode.ER_LOADING}:
                    # If Tarantool is still loading then reconnect
                    if self._reconnect_timeout > 0:
                        await self._do_reconnect(e)
                        continue
                if return_exceptions:
                    self._reconnect_coro = None
                    raise e
                else:
                    logger.exception(e)
                    if self._reconnect_timeout > 0:
                        await self._do_reconnect(e)
                        continue
            except Exception as e:
                #print(repr(e))
                if self._reconnect_timeout > 0:
                    await self._do_reconnect(e)
                    continue
                if return_exceptions:
                    self._reconnect_coro = None
                    raise e
                else:
                    logger.exception(e)

    async def _do_reconnect(self, exc=None):
        self._set_state(ConnectionState.RECONNECTING)
        logger.warning(
            'Connect to {} failed: {}. Retrying in {} seconds'.format(
                self.fingerprint, repr(exc) if exc else '',
                self._reconnect_timeout
            ))

        await asyncio.sleep(self._reconnect_timeout,
                            loop=self._loop)

    def connect(self):
        return self._connect(return_exceptions=True)

    async def disconnect(self):
        if self._state in \
                {ConnectionState.DISCONNECTING, ConnectionState.DISCONNECTED}:
            return
        self._set_state(ConnectionState.DISCONNECTING)

        logger.info('{} Disconnecting...'.format(self.fingerprint))
        waiter = _create_future(self._loop)
        if self._reconnect_coro:
            self._reconnect_coro.cancel()
            self._reconnect_coro = None

        if self._transport:
            self._disconnect_waiter = waiter
            self._transport.close()
            self._transport = None
            self._protocol = None
        else:
            waiter.set_result(True)
            self._set_state(ConnectionState.DISCONNECTED)
        return await waiter

    async def reconnect(self):
        await self.disconnect()
        await self.connect()

    @property
    def fingerprint(self):
        return 'Tarantool[{}:{}]'.format(self._host, self._port)

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
    def state(self):
        return self._state

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

    def check_connected(self):
        if not self._state == ConnectionState.CONNECTED:
            raise TarantoolNotConnectedError('Tarantool is not connected')

    def refetch_schema(self):
        return self._protocol.refetch_schema()

    def ping(self, *, timeout=0):
        self.check_connected()
        return self._protocol.ping(timeout=timeout)

    def auth(self, username, password, *, timeout=0):
        self.check_connected()
        return self._protocol.auth(username, password,
                                   timeout=timeout)

    def call16(self, func_name, args=None, *, timeout=0):
        self.check_connected()
        return self._protocol.call16(func_name, args,
                                     timeout=timeout)

    def call(self, func_name, args=None, *, timeout=0):
        self.check_connected()
        return self._protocol.call(func_name, args,
                                   timeout=timeout)

    def eval(self, expression, args=None, *, timeout=0):
        self.check_connected()
        return self._protocol.eval(expression, args,
                                   timeout=timeout)

    def select(self, space, key=None, **kwargs):
        self.check_connected()
        return self._protocol.select(space, key, **kwargs)

    def insert(self, space, t, *, replace=False, timeout=0):
        self.check_connected()
        return self._protocol.insert(space, t,
                                     replace=replace, timeout=timeout)

    def replace(self, space, t, *, timeout=0):
        self.check_connected()
        return self._protocol.replace(space, t,
                                      timeout=timeout)

    def delete(self, space, key, **kwargs):
        self.check_connected()
        return self._protocol.delete(space, key, **kwargs)

    def update(self, space, key, operations, **kwargs):
        self.check_connected()
        return self._protocol.update(space, key, operations, **kwargs)

    def upsert(self, space, t, operations, **kwargs):
        self.check_connected()
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
