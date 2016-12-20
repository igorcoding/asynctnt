import asyncio
import os

from asynctnt import protocol

__all__ = (
    'Connection'
)


class Connection:
    __slots__ = (
        '_host', '_port', '_username', '_password',
        '_connect_timeout', '_reconnect_timeout', 'loop',
        '_transport', '_protocol'
    )
    
    def __init__(self, *,
                 host=None,
                 port=None,
                 username=None,
                 password=None,
                 connect_timeout=60,
                 reconnect_timeout=None,
                 loop=None):
        
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._connect_timeout = connect_timeout
        self._reconnect_timeout = reconnect_timeout
        self.loop = loop or asyncio.get_event_loop()
        
        self._transport = None
        self._protocol = None
    
    async def connect(self):
        is_unix = self._host.startswith('unix/')
        
        addr = (self._host, self._port)
        opts = {
            'username': self._username,
            'password': self._password
        }
        connected_fut = _create_future(self.loop)
        if is_unix:
            unix_path = self._port
            assert unix_path, 'No unix file path specified'
            assert os.path.isfile(unix_path), 'Unix socket `{}` not found'.format(unix_path)
            
            conn = self.loop.create_unix_connection(
                lambda: protocol.Protocol(addr, opts=opts, connected_fut=connected_fut, loop=self.loop),
                unix_path)
        else:
            conn = self.loop.create_connection(
                lambda: protocol.Protocol(addr, opts=opts, connected_fut=connected_fut, loop=self.loop),
                self._host, self._port)

        try:
            tr, pr = await asyncio.wait_for(conn, timeout=self._connect_timeout, loop=self.loop)
        except (OSError, asyncio.TimeoutError):
            raise

        try:
            await connected_fut
        except:
            tr.close()
            raise
        
        pr.set_connection(self)
        self._transport = tr
        self._protocol = pr

    async def disconnect(self):
        pass
    
    async def reconnect(self):
        await self.disconnect()
        await self.connect()
        
    @property
    def version(self):
        return self._protocol.version
        
    def ping(self):
        return self._protocol.ping()


def _create_future(loop):
    try:
        return loop.create_future()
    except AttributeError:
        return asyncio.Future(loop=loop)
