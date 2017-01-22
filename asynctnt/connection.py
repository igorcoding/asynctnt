import asyncio
import os

import logging

# from asynctnt import protocol
from asynctnt.ciproto import protocol

__all__ = (
    'Connection'
)

logger = logging.getLogger(__package__)


class Connection:
    __slots__ = (
        '_host', '_port', '_username', '_password', '_fetch_schema',
        '_connect_timeout', '_reconnect_timeout', '_request_timeout', 'loop',
        '_transport', '_protocol', '_closing', '_disconnect_waiter'
    )
    
    def __init__(self, *,
                 host=None,
                 port=None,
                 username=None,
                 password=None,
                 fetch_schema=True,
                 connect_timeout=60,
                 request_timeout=5,
                 reconnect_timeout=1./3.,
                 loop=None):
        
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._fetch_schema = fetch_schema
        
        self._connect_timeout = connect_timeout
        self._reconnect_timeout = reconnect_timeout or 0
        self._request_timeout = request_timeout
        
        self.loop = loop or asyncio.get_event_loop()
        
        self._transport = None
        self._protocol = None
        
        self._closing = False
        self._disconnect_waiter = None
    
    async def connect(self):
        is_unix = self._host.startswith('unix/')
        
        opts = {
            'username': self._username,
            'password': self._password,
            'fetch_schema': self._fetch_schema
        }
        connected_fut = _create_future(self.loop)
        
        def connection_lost(exc):
            if self._reconnect_timeout > 0 and not self._closing:
                logger.info('Tarantool[%s:%s] Starting reconnecting',
                            self._host, self._port)
                asyncio.ensure_future(self.connect(), loop=self.loop)
            else:
                self._closing = False
                if self._disconnect_waiter:
                    self._disconnect_waiter.set_result(True)
                    self._disconnect_waiter = None
        
        def protocol_factory():
            return protocol.Protocol(host=self._host, port=self._port,
                                     username=self._username,
                                     password=self._password,
                                     fetch_schema=self._fetch_schema,
                                     connected_fut=connected_fut,
                                     on_connection_lost=connection_lost,
                                     loop=self.loop)

        while True:
            try:
                if is_unix:
                    unix_path = self._port
                    assert unix_path, \
                        'No unix file path specified'
                    assert os.path.isfile(unix_path), \
                        'Unix socket `{}` not found'.format(unix_path)
                    
                    conn = self.loop.create_unix_connection(protocol_factory,
                                                            unix_path)
                else:
                    conn = self.loop.create_connection(protocol_factory,
                                                       self._host, self._port)
                
                try:
                    tr, pr = await asyncio.wait_for(
                        conn, timeout=self._connect_timeout, loop=self.loop)
                except (OSError, asyncio.TimeoutError):
                    raise
                
                logger.info('Connected successfully to Tarantool[%s:%s]',
                            self._host, self._port)
                
                try:
                    await connected_fut
                except:
                    tr.close()
                    raise
            
                # pr.set_connection(self)
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
                                        loop=self.loop)
                else:
                    raise
    
    def disconnect(self):
        logger.info('Disconnecting from Tarantool[{}:{}]'.format(self._host,
                                                                 self._port))
        self._closing = True
        waiter = _create_future(self.loop)
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
    def version(self):
        return self._protocol.version
    
    @property
    def is_connected(self):
        if self._protocol is None:
            return False
        return self._protocol.is_connected()
    
    def __getattr__(self, name):
        # Proxy commands.
        # if name not in iproto.all_requests:
        #     raise AttributeError('Request \'{}\' not found'.format(name))
        
        return getattr(self._protocol, name)


def _create_future(loop):
    try:
        return loop.create_future()
    except AttributeError:
        return asyncio.Future(loop=loop)
