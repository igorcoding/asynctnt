import abc
import asyncio
import asyncio.subprocess
import functools
import logging
import math
import os
import random
import re
import select
import shutil
import socket
import string
import subprocess
import time
from typing import Optional

import yaml

from threading import Thread

__all__ = (
    'TarantoolInstanceProtocol', 'TarantoolInstance',
    'TarantoolAsyncInstance', 'TarantoolSyncInstance',
    'TarantoolSyncDockerInstance'
)

from asynctnt.utils import get_running_loop

VERSION_STRING_REGEX = re.compile(r'\s*([\d.]+).*')


class TarantoolInstanceProtocol(asyncio.SubprocessProtocol):
    def __init__(self, tnt, on_exit):
        super().__init__()
        self._tnt = tnt
        self._on_exit = on_exit
        self._transport = None

    @property
    def logger(self):
        return self._tnt.logger

    @property
    def pid(self):
        return self._transport.get_pid() if self._transport else None

    def connection_made(self, transport):
        self.logger.info('Process started')
        self._transport = transport

    def pipe_data_received(self, fd, data):
        if not data:
            return
        line = data.decode()
        line = line.replace('\r', '')
        lines = line.split('\n')
        for line in lines:
            line = line.replace('\n', '')
            line = line.strip()
            if line:
                self.logger.info('=> %s', line)

    def process_exited(self):
        return_code = self._transport.get_returncode()
        if callable(self._on_exit):
            self._on_exit(return_code)

    @property
    def returncode(self):
        return self._transport.get_returncode()

    async def wait(self):
        """Wait until the process exit and return the process return code.

        This method is a coroutine."""
        return await self._transport._wait()

    def send_signal(self, signal):
        self._transport.send_signal(signal)

    def terminate(self):
        self._transport.terminate()

    def kill(self):
        self._transport.kill()


class TarantoolInstance(metaclass=abc.ABCMeta):
    def __init__(self, *,
                 host='127.0.0.1',
                 port=3301,
                 console_host=None,
                 console_port=3302,
                 replication_source=None,
                 title=None,
                 logger=None,
                 log_level=5,
                 slab_alloc_arena=0.1,
                 wal_mode='none',
                 root=None,
                 specify_work_dir=True,
                 cleanup=True,
                 initlua_template=None,
                 applua='-- app.lua --',
                 timeout=5.,
                 command_to_run='tarantool',
                 command_args=None):
        """

        :param host: The host which Tarantool instance is going
                     to be listening on (default = 127.0.0.1)
        :param port: The port which Tarantool instance is going
                     to be listening on (default = 3301)
        :param console_host: The host which Tarantool console is going
                             to be listening on (to execute admin commands)
                             (default = host)
        :param console_port: The port which Tarantool console is going
                             to be listening on (to execute admin commands)
                             (default = 3302)
        :param replication_source: The replication source string.
                                   If it's None, then replication_source=nil
                                   in box.cfg
        :param title: Tarantool instance title (substitutes
                      into custom_proc_title). (default = "tnt[host:port]")
        :param logger: logger, where all messages are logged to.
                       default logger name = Tarantool[host:port]
        :param log_level: Tarantool's log_level (default = 5)
        :param slab_alloc_arena: Tarantool's slab_alloc_arena (default = 0.1)
        :param wal_mode: Tarantool's wal_mode (default = 'none')
        :param root: Tarantool's work_dir location
        :param specify_work_dir: Specify or not the workdir of Tarantool
        :param cleanup: do cleanup or not
        :param initlua_template: The initial init.lua template
                                 (default can be found in
                                 _create_initlua_template function)
        :param applua: Any extra lua script (a string)
                       (default = '-- app.lua --')
        :param timeout: Timeout in seconds - how much to wait for tarantool
                        to become active
        :param command_to_run: command exe
        :param command_args: command args
        """

        self._host = host
        self._port = port
        self._console_host = console_host or host
        self._console_port = console_port
        self._replication_source = replication_source
        self._title = title or self._generate_title()
        self._logger = logger or logging.getLogger(self.fingerprint)
        self._log_level = log_level
        self._slab_alloc_arena = slab_alloc_arena
        self._wal_mode = wal_mode
        self._root = root or self._generate_root_folder_name()
        self._specify_work_dir = specify_work_dir
        self._cleanup = cleanup

        self._initlua_template = initlua_template or \
                                 self._create_initlua_template()
        self._applua = applua
        self._command_to_run = command_to_run
        self._command_args = command_args

        self._timeout = timeout
        self._is_running = False

    @property
    def replication_source(self):
        return self._replication_source

    @replication_source.setter
    def replication_source(self, value):
        self._replication_source = value

    def _random_string(self,
                       length, *,
                       source=string.ascii_uppercase
                              + string.ascii_lowercase
                              + string.digits):
        return ''.join(random.choice(source) for _ in range(length))

    def _generate_title(self):
        return 'tnt[{}:{}]'.format(self._host, self._port)

    def _generate_root_folder_name(self):
        cwd = os.getcwd()
        path = None
        while path is None or os.path.isdir(path):
            folder_name = '__tnt__' + \
                          self._random_string(10)
            path = os.path.join(cwd, folder_name)
        return path

    @staticmethod
    def get_random_port():
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(('', 0))
        port = sock.getsockname()[1]
        sock.close()
        return port

    def _create_initlua_template(self):
        return """
            local function check_version(expected, version)
                -- from tarantool/queue compat.lua
                local fun = require 'fun'
                local iter, op  = fun.iter, fun.operator
                local function split(self, sep)
                    local sep, fields = sep or ":", {}
                    local pattern = string.format("([^%s]+)", sep)
                    self:gsub(pattern, function(c) table.insert(fields, c) end)
                    return fields
                end

                local function reducer(res, l, r)
                    if res ~= nil then
                        return res
                    end
                    if tonumber(l) == tonumber(r) then
                        return nil
                    end
                    return tonumber(l) > tonumber(r)
                end

                local function split_version(version_string)
                    local vtable  = split(version_string, '.')
                    local vtable2 = split(vtable[3],  '-')
                    vtable[3], vtable[4] = vtable2[1], vtable2[2]
                    return vtable
                end

                local function check_version_internal(expected, version)
                    version = version or _TARANTOOL
                    if type(version) == 'string' then
                        version = split_version(version)
                    end
                    local res = iter(version):zip(expected)
                                             :reduce(reducer, nil)
                    if res or res == nil then res = true end
                    return res
                end

                return check_version_internal(expected, version)
            end
            local cfg = {
              listen = "${host}:${port}",
              wal_mode = "${wal_mode}",
              custom_proc_title = "${custom_proc_title}",
              slab_alloc_arena = ${slab_alloc_arena},
              work_dir = ${work_dir},
              log_level = ${log_level}
            }
            if check_version({1, 7}, _TARANTOOL) then
                cfg.replication = ${replication_source}
            else
                local repl = ${replication_source}
                if type(repl) == 'table' then
                    repl = table.concat(repl, ',')
                end
                cfg.replication_source = repl
            end
            require('console').listen("${console_host}:${console_port}")
            box.cfg(cfg)
            box.schema.user.grant("guest", "read,write,execute", "universe",
                                  nil, {if_not_exists = true})
            ${applua}
        """

    def _render_initlua(self):
        template = string.Template(self._initlua_template)
        if not self._replication_source:
            replication = 'nil'
        elif isinstance(self._replication_source, str):
            replication = '"{}"'.format(self._replication_source)
        elif isinstance(self._replication_source, (list, tuple)):
            replication = ['"{}"'.format(e) for e in self._replication_source]
            replication = ",".join(replication)
            replication = "{" + replication + "}"
        else:
            raise TypeError('replication is of unsupported type')

        work_dir = 'nil'
        if self._specify_work_dir:
            work_dir = '"' + self._root + '"'

        d = {
            'host': self._host,
            'port': self._port,
            'console_host': self._console_host,
            'console_port': self._console_port,
            'wal_mode': self._wal_mode,
            'custom_proc_title': self._title,
            'slab_alloc_arena': self._slab_alloc_arena,
            'replication_source': replication,
            'work_dir': work_dir,
            'log_level': self._log_level,
            'applua': self._applua if self._applua else ''
        }
        return template.substitute(d)

    def _save_initlua(self, initlua):
        initlua = initlua.replace(' ' * 4, '')
        initlua_path = os.path.join(self._root, 'init.lua')
        with open(initlua_path, 'w') as f:
            f.write(initlua)
        return initlua_path

    @property
    def logger(self):
        return self._logger

    @property
    def fingerprint(self):
        return 'Tarantool[{}:{}]'.format(self._host, self._port)

    def prepare(self, recreate):
        if recreate and os.path.exists(self._root):
            shutil.rmtree(self._root, ignore_errors=True)
        if not os.path.exists(self._root):
            os.mkdir(self._root)
        initlua = self._render_initlua()
        initlua_path = self._save_initlua(initlua)
        return initlua_path

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    @property
    def console_port(self):
        return self._console_port

    @property
    def is_running(self):
        return self._is_running

    @property
    @abc.abstractmethod
    def pid(self):
        raise NotImplementedError

    @abc.abstractmethod
    def command(self, cmd, print_greeting=True):
        raise NotImplementedError

    @abc.abstractmethod
    def start(self, *, wait=True, recreate=True):
        raise NotImplementedError

    @abc.abstractmethod
    def stop(self):
        raise NotImplementedError

    @abc.abstractmethod
    def terminate(self):
        raise NotImplementedError

    @abc.abstractmethod
    def kill(self):
        raise NotImplementedError

    def cleanup(self):
        self._is_running = False
        if self._cleanup:
            shutil.rmtree(self._root, ignore_errors=True)
        if self.host == 'unix/':
            shutil.rmtree(self.port, ignore_errors=True)
        self._logger.info('Destroyed Tarantool instance (%s)', self._title)


class TcpSocket:
    BUFFER_SIZE = 1024

    def __init__(self, host, port):
        self._host = host
        self._port = port
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def connect(self):
        self._sock.connect((self._host, self._port))

    def close(self):
        if self._sock is not None:
            self._sock.close()

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def write(self, data, flags=0):
        self._sock.sendall(data, flags)

    def read(self, n, flags=0):
        buf = bytearray()
        bytes_recd = 0

        while bytes_recd < n:
            chunk = self._sock.recv(self.BUFFER_SIZE, flags)
            if chunk == b'':
                raise RuntimeError("socket connection broken")
            buf.extend(chunk)

            bytes_recd += len(chunk)
        return bytes(buf)

    def read_until(self, separator=b'', flags=0):
        buf = bytearray()
        search_start = 0
        while True:
            chunk = self._sock.recv(self.BUFFER_SIZE, flags)
            if chunk == b'':
                raise RuntimeError("socket connection broken")
            buf.extend(chunk)
            pos = buf.find(separator, search_start)
            if pos != -1:
                return bytes(buf[:(pos + len(separator))])
            search_start = len(buf) - len(separator) - 1


class TarantoolSyncInstance(TarantoolInstance):
    WAIT_TIMEOUT = 5

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._process = None
        self._logger_thread = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    @property
    def pid(self):
        return self._process.pid if self._process is not None else None

    def start(self, *, wait=True, recreate=True):
        self._logger.info('Starting Tarantool instance (%s)', self._title)
        initlua_path = self.prepare(recreate)
        self._logger.info('Launching process')

        if not self._command_args:
            args = [self._command_to_run, initlua_path]
        else:
            args = [self._command_to_run, *self._command_args]

        flags = 0
        if os.name == 'nt':
            flags |= subprocess.CREATE_NEW_PROCESS_GROUP
        self._process = subprocess.Popen(args,
                                         stdout=subprocess.PIPE,
                                         stderr=subprocess.PIPE,
                                         creationflags=flags)
        self._logger_thread = Thread(target=self._log_reader)
        self._logger_thread.start()

        if not wait:
            self._is_running = True
            return

        interval = 0.1
        attempts = math.ceil(self._timeout / interval)
        while attempts > 0:
            try:
                status = self.command('box.info.status', print_greeting=True)
                if status:
                    status = status[0]
                    if status == 'running':
                        self._logger.info('Moved to the running state')
                        break
            except (OSError, RuntimeError):
                pass
            time.sleep(interval)
            attempts -= 1
        else:
            raise TimeoutError(
                'Timeout while waiting for Tarantool to move to running state')
        self._is_running = True

    def _log_reader(self):
        def check_io():
            fds = []
            for h in [self._process.stdout,
                      self._process.stderr]:
                if h is not None and not h.closed:
                    fds.append(h)
            if not fds:
                return False
            try:
                ready_to_read = select.select(fds, [], [], 10)[0]
            except (ValueError, OSError):
                # I/O operation on a closed socket
                return False
            for io in ready_to_read:
                if io.closed:
                    continue
                try:
                    line = io.readline()
                except ValueError:
                    # assuming it's just an fd error, so skip
                    # PyMemoryView_FromBuffer(): info->buf must not be NULL
                    continue
                line = line.decode()
                if len(line) > 0:
                    self._logger.info(line[:-1])
            return True

        while self._is_running and self._process.poll() is None:
            if not check_io():
                break
        check_io()

    def stop(self):
        if self._process is not None:
            self._process.terminate()
            self._logger.info('Waiting for process to complete')
            self._wait(self.WAIT_TIMEOUT, wait=True)
            self.cleanup()

    def terminate(self):
        if self._process is not None:
            self._process.terminate()
            self._wait(self.WAIT_TIMEOUT, wait=False)
            self.cleanup()

    def kill(self):
        if self._process is not None:
            self._process.kill()
            self.cleanup()

    def _wait(self, timeout, wait=True):
        if self._process:
            if wait:
                try:
                    self._process.wait(timeout)
                except subprocess.TimeoutExpired:
                    pass

            try:
                os.kill(self._process.pid, 0)
                self._process.kill()
                self.logger.warning('Force killed %s', self.fingerprint)
            except OSError:
                pass

    def cleanup(self):
        if self._process is not None:
            for h in [self._process.stdout,
                      self._process.stderr,
                      self._process.stdin]:
                if h is not None:
                    h.close()
        self._is_running = False
        if self._logger_thread is not None:
            self._logger_thread.join()
        super().cleanup()

    def version(self) -> Optional[tuple]:
        res = self.command("box.info.version")
        if not res:
            return None
        res = res[0]
        m = VERSION_STRING_REGEX.match(res)
        if m is not None:
            ver = m.group(1)
            return tuple(map(int, ver.split('.')))

    def command(self, cmd, print_greeting=True):
        s = TcpSocket(self._console_host, self._console_port)
        try:
            s.connect()
            greeting = s.read(128).decode()
            if print_greeting:
                self._logger.info(greeting)

            if isinstance(cmd, str):
                cmd = cmd.encode('utf-8')

            s.write(cmd + b'\n')

            data = s.read_until(b'...\n').decode()
            data = yaml.full_load(data)
            return data
        finally:
            s.close()


class TarantoolAsyncInstance(TarantoolInstance):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._loop = get_running_loop(kwargs.pop('loop', None))

        self._is_stopping = False
        self._transport = None
        self._protocol = None
        self._last_return_code = None
        self._stop_event = asyncio.Event()

    @property
    def pid(self):
        return self._protocol.pid if self._protocol else None

    def prepare(self, recreate):
        self._last_return_code = None
        return super().prepare(recreate)

    def _on_process_exit(self, return_code):
        self._last_return_code = return_code
        if self._is_stopping:
            return
        self._stop_event.set()
        self.cleanup()

    async def wait_stopped(self):
        return await self._stop_event.wait()

    async def version(self):
        return await self.command("box.info.version")

    async def command(self, cmd, print_greeting=True):
        reader, writer = await asyncio.open_connection(self._console_host,
                                                       self._console_port)

        greeting = (await reader.read(128)).decode()
        if print_greeting:
            self._logger.info(greeting)

        try:
            if isinstance(cmd, str):
                cmd = cmd.encode('utf-8')
            writer.write(cmd + b'\n')
            data = (await reader.readuntil(b'...\n')).decode()
            data = yaml.full_load(data)
            return data
        finally:
            writer.close()

    async def start(self, *, wait=True, recreate=True):
        self._logger.info('Starting Tarantool instance (%s)', self._title)
        self._stop_event.clear()
        initlua_path = self.prepare(recreate)
        self._logger.info('Launching process')

        factory = functools.partial(
            TarantoolInstanceProtocol, self, self._on_process_exit)
        if not self._command_args:
            args = [initlua_path]
        else:
            args = self._command_args
        self._transport, self._protocol = await self._loop.subprocess_exec(
            factory,
            self._command_to_run, *args,
            stdin=None,
            stderr=asyncio.subprocess.PIPE
        )

        if not wait:
            self._is_running = True
            return

        interval = 0.1
        attempts = math.ceil(self._timeout / interval)
        while attempts > 0:
            if self._protocol is None or self._protocol.returncode is not None:
                raise RuntimeError(
                    '{} exited unexpectedly with exit code {}'.format(
                        self.fingerprint, self._last_return_code)
                )
            try:
                status = await self.command('box.info.status',
                                            print_greeting=False)
                if status:
                    status = status[0]
                    if status == 'running':
                        self._logger.info('Moved to the running state')
                        break
            except OSError:
                pass
            await asyncio.sleep(interval)
            attempts -= 1
        else:
            raise asyncio.TimeoutError(
                'Timeout while waiting for Tarantool to move to running state')
        self._is_running = True

    async def stop(self):
        if self._protocol is not None:
            self._is_stopping = True
            self._protocol.terminate()

            if not self._is_running:
                return

            self._logger.info('Waiting for process to complete')
            await self._protocol.wait()
            self.cleanup()

    def terminate(self):
        if self._protocol is not None:
            self._is_stopping = True
            self._protocol.terminate()
            self.cleanup()

    def kill(self):
        if self._protocol is not None:
            self._is_stopping = True
            self._protocol.kill()
            self.cleanup()

    def cleanup(self):
        return_code = self._protocol.returncode
        self._logger.info('Finished with return code %d', return_code)

        self._is_stopping = False
        if self._transport:
            self._transport.close()
        self._transport = None
        self._protocol = None
        self._stop_event.clear()

        super().cleanup()


class TarantoolSyncDockerInstance(TarantoolSyncInstance):
    def __init__(self, *,
                 docker_image=None,
                 docker_tag=None,
                 host='0.0.0.0',
                 port=3301,
                 console_host=None,
                 console_port=3302,
                 replication_source=None,
                 title=None,
                 logger=None,
                 log_level=5,
                 slab_alloc_arena=0.1,
                 wal_mode='none',
                 initlua_template=None,
                 applua='-- app.lua --',
                 timeout=10.):
        super().__init__(host=host, port=port, console_host=console_host,
                         console_port=console_port,
                         replication_source=replication_source,
                         title=title, logger=logger, log_level=log_level,
                         slab_alloc_arena=slab_alloc_arena,
                         wal_mode=wal_mode,
                         root=None, specify_work_dir=False, cleanup=True,
                         initlua_template=initlua_template,
                         applua=applua, timeout=timeout)
        self._docker_image = docker_image or 'tarantool/tarantool'
        self._docker_tag = docker_tag or '1'

        cmd = "docker run --rm " \
              "-p {port}:{port} " \
              "-p {console_port}:{console_port} " \
              "-v {root}:/opt/tarantool " \
              "{docker_image}:{docker_tag} " \
              "tarantool /opt/tarantool/init.lua"
        cmd = cmd.format(
            port=self.port,
            console_port=self.console_port,
            root=self._root,
            docker_image=self._docker_image,
            docker_tag=self._docker_tag
        )
        self.logger.debug(cmd)
        args = cmd.split(' ')
        self._command_to_run = args[0]
        self._command_args = args[1:]
