import asyncio
import enum
import functools
import os
import typing
from typing import Optional

from .exceptions import TarantoolDatabaseError, \
    ErrorCode, TarantoolNotConnectedError
from .iproto import protocol
from .log import logger

__all__ = (
    'Connection', 'connect', 'ConnectionState'
)

from .utils import get_running_loop


class ConnectionState(enum.IntEnum):
    CONNECTING = 1
    CONNECTED = 2
    RECONNECTING = 3
    DISCONNECTING = 4
    DISCONNECTED = 5


_MethodRet = typing.Union[typing.Awaitable[protocol.Response], asyncio.Future]


class Connection:
    __slots__ = (
        '_host', '_port', '_username', '_password',
        '_fetch_schema', '_auto_refetch_schema', '_initial_read_buffer_size',
        '_encoding', '_connect_timeout', '_reconnect_timeout',
        '_request_timeout', '_ping_timeout', '_loop', '_state', '_state_prev',
        '_transport', '_protocol', '_db',
        '_disconnect_waiter', '_reconnect_task',
        '_connect_lock', '_disconnect_lock',
        '_ping_task', '__create_task'
    )

    def __init__(self, *,
                 host='127.0.0.1',
                 port=3301,
                 username=None,
                 password=None,
                 fetch_schema=True,
                 auto_refetch_schema=True,
                 connect_timeout=3.,
                 request_timeout=-1.,
                 reconnect_timeout=1. / 3.,
                 ping_timeout=5.,
                 encoding=None,
                 initial_read_buffer_size=None,
                 **kwargs):

        """
            Connection constructor.

            To manipulate a Connection instance there are several functions:

                * await connect() - performs connecting, authorization and
                                    schema fetching.

                * await disconnect() - performs disconnection.

                * close() - closes connection (not a coroutine)

            Connection also supports context manager protocol, which connects
            on entering and disconnecting on leaving a block.

            So one can simply use it as follows:

            .. code-block:: python

                async with asynctnt.Connection() as conn:
                    await conn.call('box.info')

            :param host:
                    Tarantool host (pass ``unix/`` to connect to unix socket)
            :param port:
                    Tarantool port
                    (pass ``/path/to/sockfile`` to connect ot unix socket)
            :param username:
                    Username to use for auth
                    (if ``None`` you are connected as a guest)
            :param password:
                    Password to use for auth
            :param fetch_schema:
                    Pass ``True`` to be able to use spaces and indexes names in
                    data manipulation routines (default is ``True``)
            :param auto_refetch_schema:
                    If set to ``True`` then when ER_WRONG_SCHEMA_VERSION error
                    occurs on a request, schema is refetched and the initial
                    request is resent. If set to ``False`` then schema will not
                    be checked by Tarantool, so no errors will occur
            :param connect_timeout:
                    Time in seconds how long to wait for connecting to socket
            :param request_timeout:
                    Request timeout (in seconds) for all requests
                    (by default there is no timeout)
            :param reconnect_timeout:
                    Time in seconds to wait before automatic reconnect
                    (set to ``0`` or ``None`` to disable auto reconnect)
            :param ping_timeout:
                    If specified (default is 5 seconds) a background task
                    will be created which will ping Tarantool instance
                    periodically to check if it is alive and update schema
                    if it is changed
                    (set to ``0`` or ``None`` to disable this task)
            :param encoding:
                    The encoding to use for all strings
                    encoding and decoding (default is ``utf-8``)
            :param initial_read_buffer_size:
                    Initial and minimum size of read buffer in bytes.
                    Higher value means less reallocations, but higher
                    memory usage (default is 131072).
            :param loop:
                    (Deprecated) Asyncio event loop to use
        """
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._fetch_schema = False if fetch_schema is None else fetch_schema
        if auto_refetch_schema:  # None hack
            self._auto_refetch_schema = True
            if not self._fetch_schema:
                logger.warning('Setting fetch_schema to True as '
                               'auto_refetch_schema is True')
                self._fetch_schema = True
        else:
            self._auto_refetch_schema = False
        self._initial_read_buffer_size = initial_read_buffer_size
        self._encoding = encoding or 'utf-8'

        self._connect_timeout = connect_timeout
        self._reconnect_timeout = reconnect_timeout or 0
        self._request_timeout = request_timeout
        self._ping_timeout = ping_timeout or 0

        self._loop = get_running_loop(kwargs.pop('loop', None))

        self._transport = None
        self._protocol = None
        self._db = _DbMock()

        self._state = ConnectionState.DISCONNECTED
        self._state_prev = ConnectionState.DISCONNECTED
        self._disconnect_waiter = None
        self._reconnect_task = None
        self._connect_lock = asyncio.Lock()
        self._disconnect_lock = asyncio.Lock()
        self._ping_task = None

        if hasattr(self._loop, 'create_task'):
            self.__create_task = self._loop.create_task
        else:
            self.__create_task = asyncio.ensure_future

    def _set_state(self, new_state):
        if self._state != new_state:
            logger.debug('Changing state %s -> %s',
                         self._state.name, new_state.name)
            self._state_prev = self._state
            self._state = new_state

    def connection_lost(self, exc):
        if self._transport:
            self._transport.close()
        self._transport = None

        if self._disconnect_waiter:
            # disconnect() call happened
            self._disconnect_waiter.set_result(True)
            return

        # connection lost

        if self._reconnect_timeout > 0:
            # should reconnect
            self._start_reconnect(return_exceptions=False)
        else:
            # should not reconnect, close everything
            self.close()

    async def _ping_task_func(self):
        while self._state == ConnectionState.CONNECTED:
            try:
                await self.ping(timeout=2.0)
            except asyncio.CancelledError:
                break
            except Exception:
                pass

            await asyncio.sleep(self._ping_timeout)

    def _start_reconnect(self, return_exceptions=False):
        if self._state in [ConnectionState.CONNECTING,
                           ConnectionState.RECONNECTING]:
            logger.debug('%s Cannot start reconnect: already reconnecting',
                         self.fingerprint)
            return

        if self._reconnect_task:
            return

        logger.info('%s Started reconnecting', self.fingerprint)
        self._set_state(ConnectionState.RECONNECTING)

        self._reconnect_task = self.__create_task(
            self._connect(return_exceptions=return_exceptions)
        )

    def protocol_factory(self, connected_fut, cls=protocol.Protocol):
        return cls(host=self._host,
                   port=self._port,
                   username=self._username,
                   password=self._password,
                   fetch_schema=self._fetch_schema,
                   auto_refetch_schema=self._auto_refetch_schema,
                   request_timeout=self._request_timeout,
                   initial_read_buffer_size=self._initial_read_buffer_size,
                   encoding=self._encoding,
                   connected_fut=connected_fut,
                   on_connection_made=None,
                   on_connection_lost=self.connection_lost,
                   loop=self._loop)

    async def _connect(self, return_exceptions=True):
        async with self._connect_lock:
            while True:
                try:
                    ignore_states = {
                        ConnectionState.CONNECTED,
                        ConnectionState.DISCONNECTING,  # disconnect() called
                    }

                    if self._state in ignore_states:
                        self._reconnect_task = None
                        return

                    self._set_state(ConnectionState.CONNECTING)

                    async def full_connect():
                        while True:
                            connected_fut = _create_future(self._loop)

                            if self._host.startswith('unix/'):
                                unix_path = self._port
                                assert isinstance(unix_path, str), \
                                    'port must be a str instance for ' \
                                    'unix socket'
                                assert unix_path, \
                                    'No unix file path specified'
                                assert os.path.exists(unix_path), \
                                    'Unix socket `{}` not found'.format(
                                        unix_path)

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

                            tr, pr = await conn

                            try:
                                timeout = 0.05  # wait at least something
                                if self._connect_timeout is not None:
                                    timeout = self._connect_timeout / 2

                                await asyncio.wait_for(connected_fut,
                                                       timeout=timeout)
                            except asyncio.TimeoutError:
                                tr.close()
                                continue  # try again
                            except Exception:
                                tr.close()
                                raise

                            return tr, pr

                    tr, pr = await asyncio.wait_for(
                        full_connect(),
                        timeout=self._connect_timeout,
                    )

                    logger.info('%s Connected successfully', self.fingerprint)
                    self._set_state(ConnectionState.CONNECTED)

                    self._transport = tr
                    self._protocol = pr
                    self._db = self._protocol.get_common_db()
                    self._reconnect_task = None
                    self._normalize_api()

                    if self._ping_timeout:
                        self._ping_task = \
                            self.__create_task(self._ping_task_func())
                    return
                except TarantoolDatabaseError as e:
                    skip_errors = {
                        ErrorCode.ER_LOADING,
                        ErrorCode.ER_NO_SUCH_SPACE,
                        ErrorCode.ER_NO_SUCH_INDEX
                    }
                    if e.code in skip_errors:
                        # If Tarantool is still loading then reconnect
                        if self._reconnect_timeout > 0:
                            await self._wait_reconnect(e)
                            continue

                    if return_exceptions:
                        self._reconnect_task = None
                        raise e

                    logger.exception(e)
                    if self._reconnect_timeout > 0:
                        await self._wait_reconnect(e)
                        continue

                    return  # no reconnect, no return_exceptions
                except asyncio.CancelledError:
                    logger.debug("connect is cancelled")
                    self._reconnect_task = None
                    raise
                except Exception as e:
                    if self._reconnect_timeout > 0:
                        await self._wait_reconnect(e)
                        continue

                    if return_exceptions:
                        self._reconnect_task = None
                        raise e

                    logger.exception(e)
                    return  # no reconnect, no return_exceptions

    async def _wait_reconnect(self, exc=None):
        self._set_state(ConnectionState.RECONNECTING)
        logger.warning('Connect to %s failed: %s. Retrying in %f seconds',
                       self.fingerprint,
                       repr(exc) if exc else '',
                       self._reconnect_timeout)

        await asyncio.sleep(self._reconnect_timeout)

    async def connect(self) -> 'Connection':
        """
            Connect coroutine
        """
        await self._connect(True)
        return self

    async def disconnect(self):
        """
            Disconnect coroutine
        """

        async with self._disconnect_lock:
            if self._state == ConnectionState.DISCONNECTED:
                return

            self._set_state(ConnectionState.DISCONNECTING)

            logger.info('%s Disconnecting...', self.fingerprint)
            if self._reconnect_task:
                self._reconnect_task.cancel()
                self._reconnect_task = None

            if self._ping_task and not self._ping_task.done():
                self._ping_task.cancel()
                self._ping_task = None

            self._db = _DbMock()
            if self._transport:
                self._disconnect_waiter = _create_future(self._loop)
                self._transport.close()
                self._transport = None
                self._protocol = None

                await self._disconnect_waiter
                self._disconnect_waiter = None
                self._set_state(ConnectionState.DISCONNECTED)
            else:
                self._transport = None
                self._protocol = None
                self._disconnect_waiter = None
                self._set_state(ConnectionState.DISCONNECTED)

    def close(self):
        """
            Same as disconnect, but not a coroutine, i.e. it does not wait
            for disconnect to finish.
        """

        if self._state == ConnectionState.DISCONNECTED:
            return

        self._set_state(ConnectionState.DISCONNECTING)
        logger.info('%s Disconnecting...', self.fingerprint)

        if self._reconnect_task and not self._reconnect_task.done():
            self._reconnect_task.cancel()
            self._reconnect_task = None

        if self._ping_task and not self._ping_task.done():
            self._ping_task.cancel()
            self._ping_task = None

        if self._transport:
            self._transport.close()

        self._transport = None
        self._protocol = None
        self._disconnect_waiter = None
        self._db = _DbMock()
        self._set_state(ConnectionState.DISCONNECTED)

    async def reconnect(self):
        """
            Reconnect coroutine.
            Just calls disconnect() and connect()
        """
        await self.disconnect()
        await self.connect()

    async def __aenter__(self):
        """
            Executed on entering the async with section.
            Connects to Tarantool instance.
        """
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """
            Executed on leaving the async with section.
            Disconnects from Tarantool instance.
        """
        await self.disconnect()

    @property
    def fingerprint(self) -> str:
        return 'Tarantool[{}:{}]'.format(self._host, self._port)

    @property
    def host(self) -> str:
        """
            Tarantool host
        """
        return self._host

    @property
    def port(self) -> int:
        """
            Tarantool port
        """
        return self._port

    @property
    def username(self) -> Optional[str]:
        """
            Tarantool username
        """
        return self._username

    @property
    def password(self) -> Optional[str]:
        """
            Tarantool password
        """
        return self._password

    @property
    def fetch_schema(self) -> bool:
        """
            fetch_schema flag
        """
        return self._fetch_schema

    @property
    def auto_refetch_schema(self) -> bool:
        """
            auto_refetch_schema flag
        """
        return self._auto_refetch_schema

    @property
    def encoding(self) -> str:
        """
            Connection encoding
        """
        return self._encoding

    @property
    def reconnect_timeout(self) -> float:
        """
            Reconnect timeout value
        """
        return self._reconnect_timeout

    @property
    def connect_timeout(self) -> float:
        """
            Connect timeout value
        """
        return self._connect_timeout

    @property
    def request_timeout(self) -> float:
        """
            Request timeout value
        """
        return self._request_timeout

    @property
    def version(self) -> Optional[tuple]:
        """
            Protocol version tuple. ex.: (1, 6, 7)
        """
        if self._protocol is None:
            return None
        return self._protocol.get_version()

    @property
    def loop(self):
        """
            Asyncio event loop
        """
        return self._loop

    @property
    def state(self) -> ConnectionState:
        """
            Current connection state

            :rtype: ConnectionState
        """
        return self._state

    @property
    def is_connected(self) -> bool:
        """
            Check if an underlying connection is active
        """
        if self._protocol is None:
            return False
        return self._protocol.is_connected()

    @property
    def is_fully_connected(self) -> bool:
        """
            Check if connection is fully active (performed auth
            and schema fetching)
        """
        if self._protocol is None:
            return False
        return self._protocol.is_fully_connected()

    @property
    def schema_id(self) -> Optional[int]:
        """
            Tarantool's current schema id
        """
        if self._protocol is None:
            return None
        return self._protocol.schema_id

    @property
    def initial_read_buffer_size(self) -> int:
        """
            initial_read_buffer_size value
        """
        return self._initial_read_buffer_size

    async def refetch_schema(self):
        """
            Coroutine to force refetch schema
        """
        await self._protocol.refetch_schema()

    def ping(self, *, timeout=-1.0) -> _MethodRet:
        """
            Ping request coroutine

            :param timeout: Request timeout

            :returns: :class:`asynctnt.Response` instance
        """
        return self._db.ping(timeout=timeout)

    def call16(self, func_name, args=None, *,
               timeout=-1.0, push_subscribe=False) -> _MethodRet:
        """
            Call16 request coroutine. It is a call with an old behaviour
            (return result of a Tarantool procedure is wrapped into a tuple,
            if needed)

            :param func_name: function name to call
            :param args: arguments to pass to the function (list object)
            :param timeout: Request timeout
            :param push_subscribe: Subscribe to push notifications

            :returns: :class:`asynctnt.Response` instance
        """
        return self._db.call16(func_name, args,
                               timeout=timeout,
                               push_subscribe=push_subscribe)

    def call(self, func_name, args=None, *,
             timeout=-1.0, push_subscribe=False) -> _MethodRet:
        """
            Call request coroutine. It is a call with a new behaviour
            (return result of a Tarantool procedure is not wrapped into
            an extra tuple). If you're connecting to Tarantool with
            version < 1.7, then this call method acts like a call16 method

            Examples:

            .. code-block:: pycon

                # tarantool function:
                # function f(...)
                #     return ...
                # end

                >>> await conn.call('f')
                <Response sync=3 rowcount=0 data=[]>

                >>> await conn.call('f', [20, 42])
                <Response sync=3 rowcount=2 data=[20, 42]>

            :param func_name: function name to call
            :param args: arguments to pass to the function (list object)
            :param timeout: Request timeout
            :param push_subscribe: Subscribe to push notifications

            :returns: :class:`asynctnt.Response` instance
        """
        return self._db.call(func_name, args,
                             timeout=timeout, push_subscribe=push_subscribe)

    def eval(self, expression, args=None, *,
             timeout=-1.0, push_subscribe=False) -> _MethodRet:
        """
            Eval request coroutine.

            Examples:

            .. code-block:: pycon

                >>> await conn.eval('return 42')
                <Response sync=3 rowcount=1 data=[42]>


                >>> await conn.eval('return box.info.version')
                <Response sync=3 rowcount=1 data=['2.1.1-7-gd381a45b6']>

            :param expression: expression to execute
            :param args: arguments to pass to the function, that will
                         execute your expression (list object)
            :param timeout: Request timeout
            :param push_subscribe: Subscribe to push messages

            :returns: :class:`asynctnt.Response` instance
        """
        return self._db.eval(expression, args,
                             timeout=timeout, push_subscribe=push_subscribe)

    def select(self, space, key=None, **kwargs) -> _MethodRet:
        """
            Select request coroutine.

            Examples:

            .. code-block:: pycon

                >>> await conn.select('tester')
                <Response sync=3 rowcount=2 data=[
                    <TarantoolTuple id=1 name='one'>,
                    <TarantoolTuple id=2 name='two'>
                ]>

                >>> res = await conn.select('_space', ['tester'], index='name')
                >>> res.data
                [<TarantoolTuple id=512
                                 owner=1
                                 name='tester'
                                 engine='memtx'
                                 field_count=0
                                 flags={}
                                 format=[
                                    {'name': 'id', 'type': 'unsigned'},
                                    {'name': 'name', 'type': 'string'}
                                 ]>]


            :param space: space id or space name.
            :param key: key to select
            :param offset: offset to use
            :param limit: limit to use
            :param index: index id or name
            :param iterator: one of the following

                        * iterator id (int number),
                        * :class:`asynctnt.Iterator` object
                        * string with an iterator name

            :param timeout: Request timeout

            :returns: :class:`asynctnt.Response` instance
        """
        return self._db.select(space, key, **kwargs)

    def insert(self, space, t, *, replace=False, timeout=-1) -> _MethodRet:
        """
            Insert request coroutine.

            Examples:

            .. code-block:: pycon

                # Basic usage
                >>> await conn.insert('tester', [0, 'hello'])
                <Response sync=3 rowcount=1 data=[
                    <TarantoolTuple id=0 name='hello'>
                ]>

                # Using dict as an argument tuple
                >>> await conn.insert('tester', {
                ...                     'id': 0
                ...                     'text': 'hell0'
                ...                   })
                <Response sync=3 rowcount=1 data=[
                    <TarantoolTuple id=0 name='hello'>
                ]>

            :param space: space id or space name.
            :param t: tuple to insert (list object)
            :param replace: performs replace request instead of insert
            :param timeout: Request timeout

            :returns: :class:`asynctnt.Response` instance
        """
        return self._db.insert(space, t,
                               replace=replace,
                               timeout=timeout)

    def replace(self, space, t, *, timeout=-1.0) -> _MethodRet:
        """
            Replace request coroutine. Same as insert, but replace.

            :param space: space id or space name.
            :param t: tuple to insert (list object)
            :param timeout: Request timeout

            :returns: :class:`asynctnt.Response` instance
        """
        return self._db.replace(space, t, timeout=timeout)

    def delete(self, space, key, **kwargs) -> _MethodRet:
        """
            Delete request coroutine.

            Examples:

            .. code-block:: pycon

                # Assuming tuple [0, 'hello'] is in space tester

                >>> await conn.delete('tester', [0])
                <Response sync=3 rowcount=1 data=[
                    <TarantoolTuple id=0 name='hello'>
                ]>

            :param space: space id or space name.
            :param key: key to delete
            :param index: index id or name
            :param timeout: Request timeout

            :returns: :class:`asynctnt.Response` instance
        """
        return self._db.delete(space, key, **kwargs)

    def update(self, space, key, operations, **kwargs) -> _MethodRet:
        """
            Update request coroutine.

            Examples:

            .. code-block:: pycon

                # Assuming tuple [0, 'hello'] is in space tester

                >>> await conn.update('tester', [0], [ ['=', 1, 'hi!'] ])
                <Response sync=3 rowcount=1 data=[
                    <TarantoolTuple id=0 name='hi!'>
                ]>

                # you can use fields names as well
                >>> res = await conn.update('tester', [0],
                ...                         [ ['=', 'text', 'hola'] ])
                <Response sync=3 rowcount=1 data=[
                    <TarantoolTuple id=0 name='hola'>
                ]>

            :param space: space id or space name.
            :param key: key to update
            :param operations:
                    Operations list of the following format:
                    [ [op_type, field_no, ...], ... ]. Please refer to
                    https://tarantool.org/doc/book/box/box_space.html?highlight=update#lua-function.space_object.update
                    You can use field numbers as well as their names in space
                    format as a field_no (if only fetch_schema is True).
                    If field is unknown then TarantoolSchemaError is raised.
            :param index: index id or name
            :param timeout: Request timeout

            :returns: :class:`asynctnt.Response` instance
        """
        return self._db.update(space, key, operations, **kwargs)

    def upsert(self, space, t, operations, **kwargs) -> _MethodRet:
        """
            Update request coroutine. Performs either insert or update
            (depending of either tuple exists or not)

            Examples:

            .. code-block:: pycon

                # upsert does not return anything
                >>> await conn.upsert('tester', [0, 'hello'],
                ...                   [ ['=', 1, 'hi!'] ])
                <Response sync=3 rowcount=0 data=[]>

            :param space: space id or space name.
            :param t: tuple to insert if it's not in space
            :param operations:
                    Operations list to use for update if tuple is already in
                    space. It has the same format as in update requets:
                    [ [op_type, field_no, ...], ... ]. Please refer to
                    https://tarantool.org/doc/book/box/box_space.html?highlight=update#lua-function.space_object.update
                    You can use field numbers as well as their names in space
                    format as a field_no (if only fetch_schema is True).
                    If field is unknown then TarantoolSchemaError is raised.
            :param timeout: Request timeout

            :returns: :class:`asynctnt.Response` instance
        """
        return self._db.upsert(space, t, operations, **kwargs)

    def sql(self, query, args=None, *,
            parse_metadata=True, timeout=-1.0) -> _MethodRet:
        """
            Executes an SQL statement (only for Tarantool > 2)

            Examples:

            .. code-block:: pycon

                >>> await conn.sql("select 1 as a, 2 as b")
                <Response sync=3 rowcount=1 data=[<TarantoolTuple A=1 B=2>]>

                >>> await conn.sql("select * from sql_space")
                <Response sync=3 rowcount=2 data=[
                    <TarantoolTuple ID=1 NAME='James Bond'>,
                    <TarantoolTuple ID=2 NAME='Ethan Hunt'>
                ]>

                >>> await conn.sql("select * from sql_space",
                ...                parse_metadata=False)
                <Response sync=3 rowcount=2 data=[
                    <TarantoolTuple 0=1 1='James Bond'>,
                    <TarantoolTuple 0=2 1='Ethan Hunt'>
                ]>

            :param query: SQL query
            :param args: Query arguments
            :param parse_metadata: Set to False to disable response's metadata
                                   parsing for better performance
            :param timeout: Request timeout

            :returns: :class:`asynctnt.Response` instance
        """
        return self._db.sql(query, args,
                            parse_metadata=parse_metadata,
                            timeout=timeout)

    def _normalize_api(self):
        if (1, 6) <= self.version < (1, 7):  # pragma: nocover
            Connection.call = Connection.call16

    def __repr__(self):
        return "<asynctnt.Connection host={} port={} state={}>".format(
            self.host,
            self.port,
            repr(self.state)
        )


def _create_future(loop):
    try:
        return loop.create_future()
    except AttributeError:  # pragma: nocover
        return asyncio.Future(loop=loop)


class _DbMock:
    def __getattr__(self, item):
        raise TarantoolNotConnectedError('Tarantool is not connected')


async def connect(**kwargs) -> Connection:
    """
        connect shorthand. See :class:`asynctnt.Connection` for kwargs details

        :return: :class:`asynctnt.Connection` object
    """
    return await Connection(**kwargs).connect()
