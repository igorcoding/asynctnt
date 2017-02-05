cimport cpython

from libc.stdint cimport uint32_t

import base64
import socket

import re

from asynctnt.exceptions import TarantoolDatabaseError
from asynctnt.log import logger

VERSION_STRING_REGEX = re.compile(r'\s*Tarantool\s+([\d.]+)\s+.*')


cdef class CoreProtocol:
    def __init__(self,
                 host, port,
                 encoding='utf-8'):
        self.host = host
        self.port = port
        self.encoding = encoding

        self.transport = None

        self.rbuf = bytearray()
        self.state = PROTOCOL_IDLE
        self.con_state = CONNECTION_BAD
        self.reqs = {}

        self.version = None
        self.salt = None

    cdef bint _is_connected(self):
        return self.con_state != CONNECTION_BAD

    cdef bint _is_fully_connected(self):
        return self.con_state == CONNECTION_FULL

    def is_connected(self):
        return self._is_connected()

    def is_fully_connected(self):
        return self._is_fully_connected()

    cdef void _write(self, buf):
        self.transport.write(memoryview(buf))

    cdef void _on_data_received(self, data):
        cdef:
            uint32_t rlen, curr
            const char *p
            uint32_t packet_len
            Request req
            TntResponse resp
            object waiter

        # print('received data: {}'.format(data))
        if not cpython.PyBytes_CheckExact(data):
            raise BufferError('_on_data_received: expected bytes object')
        data_bytes = <bytes>data

        data_len = cpython.Py_SIZE(data_bytes)
        if data_len == 0:
            return

        self.rbuf.extend(data_bytes)

        rlen = <uint32_t>cpython.Py_SIZE(self.rbuf)
        if self.state == PROTOCOL_GREETING:
            if rlen < IPROTO_GREETING_SIZE:
                # not enough for greeting
                return
            self._process__greeting()
            self.rbuf = self.rbuf[IPROTO_GREETING_SIZE:]
        elif self.state == PROTOCOL_NORMAL:
            p = PyByteArray_AS_STRING(self.rbuf)
            curr = 0

            while rlen - curr >= 5:
                p = &p[1]  # skip to 2nd byte of packet length
                packet_len = mp_load_u32(&p)

                if rlen - curr < 5 + packet_len:
                    # not enough to read whole packet
                    break

                curr += 5 + packet_len
                resp = response_parse(p, packet_len, self.encoding)
                p = &p[packet_len]

                sync = resp.sync

                req = self.reqs.get(sync)
                if req is None:
                    logger.warning('sync {} not found'.format(sync))
                    continue

                del self.reqs[sync]

                waiter = req.waiter
                if waiter is not None \
                        and not waiter.done() \
                        and not waiter.cancelled():
                    if resp.code != 0:
                        waiter.set_exception(
                            TarantoolDatabaseError(resp.code, resp.errmsg))
                    else:
                        waiter.set_result(resp)

            if curr > 0:
                self.rbuf = self.rbuf[curr:]
        else:
            # TODO: raise exception
            pass

    cdef void _process__greeting(self):
        ver_length = TARANTOOL_VERSION_LENGTH
        rbuf = self.rbuf
        self.version = self._parse_version(rbuf[:ver_length])
        self.salt = base64.b64decode(
            rbuf[ver_length:ver_length + SALT_LENGTH])[:SCRAMBLE_SIZE]
        self.state = PROTOCOL_NORMAL
        self._on_greeting_received()

    def _parse_version(self, version):
        m = VERSION_STRING_REGEX.match(version.decode('ascii'))
        if m is not None:
            ver = m.group(1)
            return tuple(map(int, ver.split('.')))

    cdef void _on_connection_made(self):
        pass

    cdef void _on_connection_lost(self, exc):
        cdef Request req
        for sync in self.reqs:
            req = self.reqs[sync]
            waiter = req.waiter
            if waiter and not waiter.done():
                if exc is None:
                    waiter.set_result(None)
                else:
                    waiter.set_exception(exc)

    cdef void _on_greeting_received(self):
        pass

    # asyncio callbacks

    def data_received(self, data):
        self._on_data_received(data)

    def connection_made(self, transport):
        print('coreproto: connection_made')
        self.transport = transport
        self.con_state = CONNECTION_CONNECTED

        sock = transport.get_extra_info('socket')
        if sock is not None and \
                (not hasattr(socket, 'AF_UNIX') or
                         sock.family != socket.AF_UNIX):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        self.state = PROTOCOL_GREETING
        self._on_connection_made()

    def connection_lost(self, exc):
        print('coreproto: connection_lost')
        self.con_state = CONNECTION_BAD
        # self.schema = None
        self.version = None
        self.salt = None
        self.rbuf = bytearray()

        self._on_connection_lost(exc)
