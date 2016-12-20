import asyncio
import base64
import enum
import socket

import logging

import re

import msgpack
from tarantool import DatabaseError
from tarantool.request import RequestPing
from tarantool.response import Response

from asynctnt.const import IPROTO_GREETING_SIZE, TARANTOOL_VERSION_LENGTH, SALT_LENGTH


class ProtocolState(enum.IntEnum):
    IDLE = 0
    GREETING = 1
    AUTH = 2
    NORMAL = 3
    
logger = logging.getLogger(__package__)


class BaseProtocol:
    VERSION_STRING_REGEX = re.compile(r'\s*Tarantool\s+([\d.]+)\s+.*')
    
    def __init__(self, addr, opts, connected_fut, loop):
        self._write_buf = b''
        self.addr = addr
        self.opts = opts
        self._connected_fut = connected_fut
        self.loop = loop
        
        self._connection = None
        self.transport = None
        
        self._reqs = {}
        self._state = ProtocolState.IDLE
        self._rbuf = bytes()
        self._sync = 0
        
        self.version = None
        self._salt = None
        # self._ruse = 0
        
        self.encoding = None
        
        try:
            self.create_future = self.loop.create_future
        except AttributeError:
            self.create_future = self._create_future_fallback

    def _create_future_fallback(self):
        return asyncio.Future(loop=self.loop)
    
    def set_connection(self, connection):
        self._connection = connection
        
    def data_received(self, data):
        if self._state == ProtocolState.GREETING:
            self._rbuf += data
            if len(self._rbuf) < IPROTO_GREETING_SIZE:
                logger.debug('greeting not enough')
                return
            self._process__greeting()
            self._connected_fut.set_result(True)  # FIXME: need also to set it after auth
            self._rbuf = self._rbuf[IPROTO_GREETING_SIZE:]
        elif self._state == ProtocolState.NORMAL:
            self._rbuf += data
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
                    logger.error("aio git happens: {r}", response)
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
        
    def _process__greeting(self):
        rbuf = self._rbuf
        self._version = self._parse_version(rbuf[:TARANTOOL_VERSION_LENGTH])
        self._salt = base64.b64decode(rbuf[TARANTOOL_VERSION_LENGTH:TARANTOOL_VERSION_LENGTH + SALT_LENGTH])

        self._state = ProtocolState.NORMAL
        if self.opts:
            username = self.opts.get('username')
            password = self.opts.get('password')
            
            if username and password:
                print('authorize')
                # self.authorize()
                return
            
    def _parse_version(self, version):
        m = self.VERSION_STRING_REGEX.match(version.decode('ascii'))
        if m is not None:
            ver = m.group(1)
            return tuple(map(int, ver.split('.')))

    def connection_made(self, transport):
        self.transport = transport
    
        sock = transport.get_extra_info('socket')
        if sock is not None and \
                (not hasattr(socket, 'AF_UNIX') or sock.family != socket.AF_UNIX):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    
        try:
            self._state = ProtocolState.GREETING
            self._connect()
        except Exception as ex:
            transport.abort()
            # self.con_status = CONNECTION_BAD
            # self._set_state(PROTOCOL_FAILED)
            # self._on_error(ex)
            
    def _connect(self):
        pass
    
    async def _execute(self, request):
        sync = request.sync
        waiter = self._reqs[sync]
        self._write_buf += bytes(request)
        b = self._write_buf

        self._write_buf = b""
        self.transport.write(b)
        
        return await waiter
    
    def ping(self):
        request = RequestPing(self)
        return self._execute(request)
        
    def generate_sync(self):
        self._sync += 1
        self._reqs[self._sync] = self.create_future()
        return self._sync


class Protocol(BaseProtocol, asyncio.Protocol):
    pass
