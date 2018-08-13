cimport cpython
cimport cpython.dict

from cpython.ref cimport PyObject

from libc.stdint cimport uint32_t, uint64_t

import base64
import socket

import re

from asynctnt.exceptions import TarantoolDatabaseError
from asynctnt.log import logger

VERSION_STRING_REGEX = re.compile(r'\s*Tarantool\s+([\d.]+)\s+.*')


cdef class CoreProtocol:
    def __init__(self,
                 host, port,
                 encoding=None,
                 initial_read_buffer_size=None):
        self.host = host
        self.port = port

        encoding = encoding or b'utf-8'
        if isinstance(encoding, str):
            self.encoding = encoding.encode()
        elif isinstance(self.encoding, bytes):
            self.encoding = encoding
        else:
            raise TypeError('encoding must be either str or bytes')

        initial_read_buffer_size = initial_read_buffer_size or 0x20000
        self.transport = None

        self.rbuf = ReadBuffer.new(encoding, initial_read_buffer_size)
        self.state = PROTOCOL_IDLE
        self.con_state = CONNECTION_BAD

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

    def get_version(self):
        return self.version

    cdef void _write(self, buf) except *:
        self.transport.write(memoryview(buf))

    cdef void _on_data_received(self, data):
        cdef:
            size_t ruse, curr
            const char *p
            const char *q
            uint32_t packet_len

            char *data_str
            ssize_t data_len
            ssize_t buf_len

        if not cpython.PyBytes_CheckExact(data):
            raise BufferError('_on_data_received: expected bytes object')

        data_str = NULL
        data_len = 0
        cpython.bytes.PyBytes_AsStringAndSize(<bytes>data,
                                              &data_str,
                                              &data_len)
        if data_len == 0:
            return

        self.rbuf.extend(data_str, data_len)

        if self.state == PROTOCOL_GREETING:
            if self.rbuf.use < IPROTO_GREETING_SIZE:
                # not enough for greeting
                return
            self._process__greeting()
            self.rbuf.move(IPROTO_GREETING_SIZE)
        elif self.state == PROTOCOL_NORMAL:
            p = self.rbuf.buf
            end = &self.rbuf.buf[self.rbuf.use]

            while p < end:
                q = p  # q is temporary to parse packet length
                buf_len = end - p
                if buf_len < 5:
                    # not enough
                    break

                q = &q[1]  # skip to 2nd byte of packet length
                packet_len = mp_load_u32(&q)

                if buf_len < 5 + packet_len:
                    # not enough to read an entire packet
                    break

                p = &p[5]  # skip length header
                self._on_response_received(p, packet_len)
                p = &p[packet_len]

                if p == end:
                    self.rbuf.use = 0
                    break

            self.rbuf.use = end - p
            if self.rbuf.use > 0:
                self.rbuf.move_offset(p - self.rbuf.buf, self.rbuf.use)
        else:
            # TODO: raise exception
            pass

    cdef void _process__greeting(self):
        cdef size_t ver_length = TARANTOOL_VERSION_LENGTH
        rbuf = self.rbuf
        self.version = self._parse_version(self.rbuf.get_slice_end(ver_length))
        self.salt = base64.b64decode(
            self.rbuf.get_slice(ver_length,
                                ver_length + SALT_LENGTH)
        )[:SCRAMBLE_SIZE]
        self.state = PROTOCOL_NORMAL
        self._on_greeting_received()

    def _parse_version(self, version):
        m = VERSION_STRING_REGEX.match(version.decode('ascii'))
        if m is not None:
            ver = m.group(1)
            return tuple(map(int, ver.split('.')))

    cdef void _on_greeting_received(self):
        pass

    cdef void _on_response_received(self, const char *buf, uint32_t buf_len):
        pass

    cdef void _on_connection_made(self):
        pass

    cdef void _on_connection_lost(self, exc):
        pass

    # asyncio callbacks

    def data_received(self, data):
        self._on_data_received(data)

    def connection_made(self, transport):
        self.transport = transport
        self.con_state = CONNECTION_CONNECTED

        sock = transport.get_extra_info('socket')
        if sock is not None \
                and (not hasattr(socket, 'AF_UNIX')
                     or sock.family != socket.AF_UNIX):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        self.state = PROTOCOL_GREETING
        self._on_connection_made()

    def connection_lost(self, exc):
        self.con_state = CONNECTION_BAD
        # self.schema = None
        self.version = None
        self.salt = None
        self.rbuf = None

        self._on_connection_lost(exc)
