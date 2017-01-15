import asyncio
import base64
import enum
import socket

import logging

import re

import msgpack
from tarantool import DatabaseError
from tarantool.response import Response
from tarantool.utils import check_key

from asynctnt.const import IPROTO_GREETING_SIZE, TARANTOOL_VERSION_LENGTH, SALT_LENGTH, SCRAMBLE_SIZE
from asynctnt.iproto import IProto
from asynctnt.schema import Schema, parse_schema


class ProtocolState(enum.IntEnum):
    IDLE = 0
    GREETING = 1
    NORMAL = 2


class ConnectionState(enum.IntEnum):
    BAD = 0
    CONNECTED = 1
    FULLY_CONNECTED = 2


logger = logging.getLogger(__package__)


class TarantoolError(Exception):
    pass


class NotConnectedError(TarantoolError):
    pass


class ConnectionLostError(TarantoolError):
    pass


class BaseProtocol:
    VERSION_STRING_REGEX = re.compile(r'\s*Tarantool\s+([\d.]+)\s+.*')
    _SPACE_VSPACE = 281
    _SPACE_VINDEX = 289
    
    __slots__ = (
        '_host', '_port', '_opts', '_on_connection_lost', '_loop',
        '_request_timeout', '_reconnect_timeout',
        '_connection', '_connected_fut', '_transport',
        '_reqs', '_state', '_con_state', '_rbuf', '_iproto', 'schema',
        'version', 'salt',
        'encoding', 'error',
        'create_future',
    
    )
    
    def __init__(self, host, port, opts, connected_fut, on_connection_lost, loop, **kwargs):
        self._host = host
        self._port = port
        self._opts = opts
        self._on_connection_lost = on_connection_lost
        self._loop = loop
        self._request_timeout = kwargs.get('request_timeout', 3)  # 0 means no timeout
        self._reconnect_timeout = kwargs.get('reconnect_timeout', 0)
        
        self._connection = None
        self._connected_fut = connected_fut
        self._transport = None
        
        self._reqs = {}
        self._state = ProtocolState.IDLE
        self._con_state = ConnectionState.BAD
        self._rbuf = bytearray()
        self._iproto = IProto()
        self.schema = None
        
        self.version = None
        self.salt = None
        
        # Compatibility with tarantool package
        self.encoding = None
        self.error = False
        
        try:
            self.create_future = self._loop.create_future
        except AttributeError:
            self.create_future = self._create_future_fallback
    
    def _create_future_fallback(self):  # pragma: no cover
        return asyncio.Future(loop=self._loop)
    
    def set_connection(self, connection):
        self._connection = connection
    
    def is_connected(self):
        return self._con_state != ConnectionState.BAD
    
    def is_fully_connected(self):
        return self._con_state == ConnectionState.FULLY_CONNECTED
    
    def data_received(self, data):
        if not data:
            return
        
        self._rbuf.extend(data)
        if self._state == ProtocolState.GREETING:
            if len(self._rbuf) < IPROTO_GREETING_SIZE:
                logger.debug('greeting not enough')
                return
            self._process__greeting()
            self._rbuf = self._rbuf[IPROTO_GREETING_SIZE:]
        elif self._state == ProtocolState.NORMAL:
            rbuf = self._rbuf
            len_buf = len(rbuf)
            curr = 0
            
            while len_buf - curr >= 5:
                length_pack = rbuf[curr:curr + 5]
                length = msgpack.unpackb(length_pack)
                
                if len_buf - curr < 5 + length:
                    break
                
                body = rbuf[curr + 5:curr + 5 + length]
                curr += 5 + length
                
                response = Response(self, body)  # unpack response
                
                sync = response.sync
                if sync not in self._reqs:
                    logger.error("Tarantool[{}:{}] request with sync {} not found: {}".format(
                        self._host, self._port, sync, response
                    ))
                    continue
                
                waiter = self._reqs[sync]
                if not waiter.cancelled():
                    if response.return_code != 0:
                        waiter.set_exception(DatabaseError(response.return_code, response.return_message))
                    else:
                        waiter.set_result(response)
                
                del self._reqs[sync]
            
            # one cut for buffer
            
            if curr:
                self._rbuf = rbuf[curr:]
        else:
            pass
    
    def eof_received(self):
        print('eof')
    
    def _process__greeting(self):
        ver_length = TARANTOOL_VERSION_LENGTH
        rbuf = self._rbuf
        self.version = self._parse_version(rbuf[:ver_length])
        self.salt = base64.b64decode(rbuf[ver_length:ver_length + SALT_LENGTH])[:SCRAMBLE_SIZE]
        
        self._state = ProtocolState.NORMAL
        if self._opts:
            username = self._opts.get('username')
            password = self._opts.get('password')
            fetch_schema = self._opts.get('fetch_schema', False)
            
            if username and password:
                self._do_auth(username, password)
                return
            elif fetch_schema:
                self._do_fetch_schema()
                return
        
        self._connected_fut.set_result(True)
        self._con_state = ConnectionState.FULLY_CONNECTED
    
    def _do_auth(self, username, password):
        fut = self.auth(username, password)
        
        def on_authorized(f):
            if f.cancelled():
                self._connected_fut.set_exception(asyncio.futures.CancelledError())
                self._con_state = ConnectionState.BAD
                return
            e = f.exception()
            if not e:
                logger.debug('Tarantool[{}:{}] Authorized successfully'.format(self._host, self._port))
                fetch_schema = self._opts.get('fetch_schema', False)
                if fetch_schema:
                    self._do_fetch_schema()
                else:
                    self._connected_fut.set_result(True)
                    self._con_state = ConnectionState.FULLY_CONNECTED
            else:
                logger.error('Tarantool[{}:{}] Authorization failed'.format(self._host, self._port))
                self._connected_fut.set_exception(e)
                self._con_state = ConnectionState.BAD
        
        fut.add_done_callback(on_authorized)
        return fut
    
    def _do_fetch_schema(self):
        fut_vspace = self.select(self._SPACE_VSPACE)
        fut_vindex = self.select(self._SPACE_VINDEX)
        
        def on_fetch(f):
            if f.cancelled():
                self._connected_fut.set_exception(asyncio.futures.CancelledError())
                self._con_state = ConnectionState.BAD
                return
            e = f.exception()
            if not e:
                spaces, indexes = f.result()
                logger.debug('Tarantool[{}:{}] Schema fetch succeeded. Spaces: {}, Indexes: {}.'.format(
                    self._host, self._port, len(spaces), len(indexes)))
                self.schema = parse_schema(spaces, indexes)
                self._connected_fut.set_result(True)
                self._con_state = ConnectionState.FULLY_CONNECTED
            else:
                logger.error('Tarantool[{}:{}] Schema fetch failed'.format(self._host, self._port))
                self._con_state = ConnectionState.BAD
                if isinstance(e, asyncio.TimeoutError):
                    self._connected_fut.set_exception(asyncio.TimeoutError('Schema fetch timeout'))
                else:
                    self._connected_fut.set_exception(e)
        
        fut = asyncio.ensure_future(
            asyncio.wait_for(
                asyncio.gather(fut_vspace, fut_vindex, return_exceptions=True,
                               loop=self._loop),
                self._request_timeout,
                loop=self._loop
            ),
            loop=self._loop
        )
        fut.add_done_callback(on_fetch)
        return fut
    
    def _parse_version(self, version):
        m = self.VERSION_STRING_REGEX.match(version.decode('ascii'))
        if m is not None:
            ver = m.group(1)
            return tuple(map(int, ver.split('.')))
    
    def connection_made(self, transport):
        self._transport = transport
        self._con_state = ConnectionState.CONNECTED
        
        sock = transport.get_extra_info('socket')
        if sock is not None and \
                (not hasattr(socket, 'AF_UNIX') or sock.family != socket.AF_UNIX):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        
        try:
            self._state = ProtocolState.GREETING
        except Exception as ex:
            transport.abort()
    
    def connection_lost(self, exc):
        logger.debug('Connection lost: {}'.format(exc))
        self._con_state = ConnectionState.BAD
        self.schema = None
        self.version = None
        self.salt = None
        self._rbuf = bytes()
        
        for sync, fut in self._reqs.items():
            if fut and not fut.cancelled() and not fut.done():
                if exc is None:
                    fut.set_result(None)
                else:
                    # fut.set_exception(ConnectionLostError('Connection to Tarantool lost'))
                    fut.set_exception(exc)
        
        if self._on_connection_lost:
            self._on_connection_lost(exc)
    
    def _execute(self, sync, request_data, *, timeout=0):
        if not self.is_connected():
            raise NotConnectedError('Tarantool is not connected')
        
        waiter = self.create_future()
        if timeout and timeout > 0:
            # Client should wait the special timeout-ed future (wrapping waiter)
            fut = asyncio.ensure_future(
                asyncio.wait_for(waiter, timeout=timeout, loop=self._loop),
                loop=self._loop
            )
        else:
            # Client should wait the waiter
            fut = waiter
        
        self._reqs[sync] = waiter
        self._transport.write(request_data)
        
        return fut
    
    def ping(self, **kwargs):
        timeout = kwargs.get('timeout')
        return self._execute(*self._iproto.ping(),
                             timeout=timeout)
    
    def auth(self, username, password, **kwargs):
        timeout = kwargs.get('timeout')
        assert self.salt, 'Salt is required'
        return self._execute(*self._iproto.auth(self.salt, username, password),
                             timeout=timeout)
    
    def call(self, func_name, *args, **kwargs):
        assert isinstance(func_name, str), 'Func name must be a str, got: {}'.format(type(func_name))
        
        if len(args) == 1 and isinstance(args[0], (list, tuple)):
            args = args[0]
        
        timeout = kwargs.get('timeout')
        return self._execute(*self._iproto.call(func_name, args),
                             timeout=timeout)
    
    def select(self, space_name, key=None, **kwargs):
        offset = kwargs.get('offset', 0)
        limit = kwargs.get('limit', 0xffffffff)
        index_name = kwargs.get('index', 0)
        iterator = kwargs.get('iterator', 0)
        timeout = kwargs.get('timeout', 0)
        
        key = check_key(key, select=True)
        
        if isinstance(space_name, str):
            sp = self.schema.get_space(space_name)
            if sp is None:
                raise Exception('Space {} not found'.format(space_name))
            space_name = sp.sid
        
        if isinstance(index_name, str):
            idx = self.schema.get_index(space_name, index_name)
            if idx is None:
                raise Exception('Index {} for space {} not found'.format(index_name, space_name))
            index_name = idx.iid
        
        return self._execute(*self._iproto.select(space_name, index_name, key, offset, limit, iterator),
                             timeout=timeout)
    
    def insert(self, space_name, values, **kwargs):
        timeout = kwargs.get('timeout')
        
        if isinstance(space_name, str):
            sp = self.schema.get_space(space_name)
            if sp is None:
                raise Exception('Space {} not found'.format(space_name))
            space_name = sp.sid
        
        return self._execute(*self._iproto.insert(space_name, values),
                             timeout=timeout)


class Protocol(BaseProtocol, asyncio.Protocol):
    pass
