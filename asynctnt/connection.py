import asyncio
import enum
import functools
import os

from asynctnt.exceptions import TarantoolDatabaseError, \
    ErrorCode, TarantoolNotConnectedError, TarantoolError
from asynctnt.iproto import protocol
from asynctnt.log import logger

__all__ = (
    'Connection', 'connect', 'ConnectionState'
)


class ConnectionState(enum.IntEnum):
    IDLE = 0
    CONNECTING = 1
    CONNECTED = 2
    RECONNECTING = 3
    DISCONNECTING = 4
    DISCONNECTED = 5


class Connection:
    __slots__ = (
        '_host', '_port', '_username', '_password',
        '_fetch_schema', '_auto_refetch_schema', '_initial_read_buffer_size',
        '_encoding', '_connect_timeout', '_reconnect_timeout',
        '_request_timeout', '_tuple_as_dict', '_loop', '_state', '_state_prev',
        '_transport', '_protocol', '_db',
        '_disconnect_waiter', '_reconnect_coro'
    )

    def __init__(self, *,
                 host='127.0.0.1',
                 port=3301,
                 username=None,
                 password=None,
                 fetch_schema=True,
                 auto_refetch_schema=True,
                 connect_timeout=60,
                 request_timeout=-1,
                 reconnect_timeout=1. / 3.,
                 tuple_as_dict=False,
                 encoding=None,
                 initial_read_buffer_size=None,
                 loop=None):

        """
            Connection constructor.

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
            :param tuple_as_dict:
                    Bool value indicating whether or not to use spaces
                    schema to decode response tuples by default. You can
                    always change this behaviour in the request itself.
                    Note: fetch_schema must be ``True``
            :param encoding:
                    The encoding to use for all strings
                    encoding and decoding (default is ``utf-8``)
            :param initial_read_buffer_size:
                    Initial and minimum size of read buffer in bytes.
                    Higher value means less reallocations, but higher
                    memory usage.
            :param loop:
                    Asyncio event loop to use
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
        if tuple_as_dict and not self._fetch_schema:
            raise TarantoolError(
                'fetch_schema must be True to be able to use '
                'unpacking tuples to dict'
            )
        self._tuple_as_dict = tuple_as_dict

        self._connect_timeout = connect_timeout
        self._reconnect_timeout = reconnect_timeout or 0
        self._request_timeout = request_timeout

        self._loop = loop or asyncio.get_event_loop()

        self._transport = None
        self._protocol = None
        self._db = _DbMock()

        self._state = ConnectionState.IDLE
        self._state_prev = ConnectionState.IDLE
        self._disconnect_waiter = None
        self._reconnect_coro = None

    def _set_state(self, new_state):
        if self._state != new_state:
            logger.debug('Changing state %s -> %s',
                         self._state.name, new_state.name)
            self._state_prev = self._state
            self._state = new_state
            return True
        return False

    def connection_made(self):
        self._set_state(ConnectionState.CONNECTED)

    def connection_lost(self, exc):
        if self._transport:
            self._transport.close()
        self._transport = None

        if self._reconnect_timeout > 0 \
                and self._state != ConnectionState.DISCONNECTING \
                and self._state != ConnectionState.DISCONNECTED:
            if self._state == ConnectionState.RECONNECTING:
                return
            self._set_state(ConnectionState.DISCONNECTING)
            self._start_reconnect(return_exceptions=False)
        else:
            self._set_state(ConnectionState.DISCONNECTED)
            if self._disconnect_waiter:
                self._disconnect_waiter.set_result(True)
                self._disconnect_waiter = None

    def __create_reconnect_coro(self, return_exceptions=False):
        if self._reconnect_coro:
            self._reconnect_coro.cancel()
        self._reconnect_coro = asyncio.ensure_future(
            self._connect(return_exceptions=return_exceptions),
            loop=self._loop
        )
        return self._reconnect_coro

    def _start_reconnect(self, return_exceptions=False):
        if self._state == ConnectionState.RECONNECTING:
            logger.info('Already in reconnecting state')
            return

        logger.info('%s Started reconnecting', self.fingerprint)
        self._set_state(ConnectionState.RECONNECTING)
        self.__create_reconnect_coro(return_exceptions)

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
                   tuple_as_dict=self._tuple_as_dict,
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

                connected_fut = _create_future(self._loop)

                if self._host.startswith('unix/'):
                    unix_path = self._port
                    assert unix_path, \
                        'No unix file path specified'
                    assert os.path.exists(unix_path), \
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

                logger.info('%s Connected successfully', self.fingerprint)
                self._set_state(ConnectionState.CONNECTED)

                self._transport = tr
                self._protocol = pr
                self._db = self._protocol.get_common_db()
                self._reconnect_coro = None
                self._normalize_api()
                return
            except TarantoolDatabaseError as e:
                if e.code in {ErrorCode.ER_LOADING}:
                    # If Tarantool is still loading then reconnect
                    if self._reconnect_timeout > 0:
                        await self._wait_reconnect(e)
                        continue
                if return_exceptions:
                    self._reconnect_coro = None
                    raise e
                else:
                    logger.exception(e)
                    if self._reconnect_timeout > 0:
                        await self._wait_reconnect(e)
                        continue
            except Exception as e:
                if self._reconnect_timeout > 0:
                    await self._wait_reconnect(e)
                    continue
                if return_exceptions:
                    self._reconnect_coro = None
                    raise e
                else:
                    logger.exception(e)

    async def _wait_reconnect(self, exc=None):
        self._set_state(ConnectionState.RECONNECTING)
        logger.warning('Connect to %s failed: %s. Retrying in %f seconds',
                       self.fingerprint,
                       repr(exc) if exc else '',
                       self._reconnect_timeout)

        await asyncio.sleep(self._reconnect_timeout,
                            loop=self._loop)

    async def connect(self):
        """
            Connect coroutine
        """
        await self.__create_reconnect_coro(True)

    async def disconnect(self):
        """
            Disconnect coroutine
        """
        if self._state in \
                {ConnectionState.DISCONNECTING, ConnectionState.DISCONNECTED}:
            return
        self._set_state(ConnectionState.DISCONNECTING)

        logger.info('%s Disconnecting...', self.fingerprint)
        waiter = _create_future(self._loop)
        if self._reconnect_coro:
            self._reconnect_coro.cancel()
            self._reconnect_coro = None

        if self._transport:
            self._disconnect_waiter = waiter
            self._transport.close()
            self._transport = None
            self._protocol = None
            self._db = _DbMock()
        else:
            waiter.set_result(True)
            self._set_state(ConnectionState.DISCONNECTED)
        return await waiter

    def close(self):
        """
            Same as disconnect, but not a coroutine, i.e. it does not wait
            for disconnect to finish.
        """
        if self._state in \
                {ConnectionState.DISCONNECTING, ConnectionState.DISCONNECTED}:
            return
        self._set_state(ConnectionState.DISCONNECTING)
        logger.info('%s Disconnecting...', self.fingerprint)

        if self._reconnect_coro:
            self._reconnect_coro.cancel()
            self._reconnect_coro = None

        if self._transport:
            self._disconnect_waiter = None
            self._transport.close()
            self._transport = None
            self._protocol = None
            self._db = _DbMock()
        self._set_state(ConnectionState.DISCONNECTED)

    async def reconnect(self):
        """
            Reconnect coroutine.
            Just calls disconnect() and connect()
        """
        await self.disconnect()
        self._set_state(ConnectionState.IDLE)
        await self.connect()

    @property
    def fingerprint(self):
        return 'Tarantool[{}:{}]'.format(self._host, self._port)

    @property
    def host(self):
        """
            Tarantool host
        """
        return self._host

    @property
    def port(self):
        """
            Tarantool port
        """
        return self._port

    @property
    def username(self):
        """
            Tarantool username
        """
        return self._username

    @property
    def password(self):
        """
            Tarantool password
        """
        return self._password

    @property
    def fetch_schema(self):
        """
            fetch_schema flag
        """
        return self._fetch_schema

    @property
    def auto_refetch_schema(self):
        """
            auto_refetch_schema flag
        """
        return self._auto_refetch_schema

    @property
    def encoding(self):
        """
            Connection encoding
        """
        return self._encoding

    @property
    def reconnect_timeout(self):
        """
            Reconnect timeout value
        """
        return self._reconnect_timeout

    @property
    def connect_timeout(self):
        """
            Connect timeout value
        """
        return self._connect_timeout

    @property
    def request_timeout(self):
        """
            Request timeout value
        """
        return self._request_timeout

    @property
    def version(self):
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
    def state(self):
        """
            Current connection state

            :rtype: ConnectionState
        """
        return self._state

    @property
    def is_connected(self):
        """
            Check if connection is active
        """
        if self._protocol is None:
            return False
        return self._protocol.is_connected()

    @property
    def schema_id(self):
        """
            Tarantool's current schema id
        """
        if self._protocol is None:
            return None
        return self._protocol.schema_id

    @property
    def initial_read_buffer_size(self):
        """
            initial_read_buffer_size value
        """
        return self._initial_read_buffer_size

    async def refetch_schema(self):
        """
            Coroutine to force refetch schema
        """
        await self._protocol.refetch_schema()

    def ping(self, *, timeout=-1):
        """
            Ping request coroutine

            :param timeout: Request timeout

            :returns: :class:`asynctnt.Response` instance 
        """
        return self._db.ping(timeout=timeout)

    def call16(self, func_name, args=None, *, timeout=-1):
        """
            Call16 request coroutine. It is a call with an old behaviour
            (return result of a Tarantool procedure is wrapped into a tuple,
            if needed)

            :param func_name: function name to call
            :param args: arguments to pass to the function (list object)
            :param timeout: Request timeout

            :returns: :class:`asynctnt.Response` instance
        """
        return self._db.call16(func_name, args,
                               timeout=timeout)

    def call(self, func_name, args=None, *, timeout=-1):
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

                >>> res = await conn.call('f')
                >>> res.body
                []

                >>> res = await conn.call('f', [20, 42])
                >>> res.body
                [20, 42]

            :param func_name: function name to call
            :param args: arguments to pass to the function (list object)
            :param timeout: Request timeout

            :returns: :class:`asynctnt.Response` instance
        """
        return self._db.call(func_name, args,
                             timeout=timeout)

    def eval(self, expression, args=None, *, timeout=-1):
        """
            Eval request coroutine.

            Examples:

            .. code-block:: pycon

                >>> res = await conn.eval('return 42')
                >>> res.body
                [42]

                >>> res = await conn.eval('return box.info.version')
                >>> res.body
                ['1.7.3-354-ge7550da']

            :param expression: expression to execute
            :param args: arguments to pass to the function, that will
                         execute your expression (list object)
            :param timeout: Request timeout
            
            :returns: :class:`asynctnt.Response` instance
        """
        return self._db.eval(expression, args,
                             timeout=timeout)

    def select(self, space, key=None, **kwargs):
        """
            Select request coroutine.

            Examples:

            .. code-block:: pycon

                >>> await conn.select('tester')
                <Response: code=0, sync=7, body_len=4>

                >>> res = await conn.select('_space', {'tester'}, index='name')
                >>> res.body
                [[512, 1, 'tester', 'memtx', 0, {}, [{'name': 'id', 'type': 'unsigned'}, {'name': 'text', 'type': 'string'}]]]

                >>> res = await conn.select('_space', {'tester'},
                ...                         index='name',
                ...                         tuple_as_dict=True)
                >>> res.body2yaml()
                - engine: memtx
                  field_count: 0
                  flags: {}
                  format:
                  - {name: id, type: unsigned}
                  - {name: text, type: string}
                  id: 512
                  name: tester
                  owner: 1


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
            :param tuple_as_dict: Decode tuple according to schema

            :returns: :class:`asynctnt.Response` instance
        """
        return self._db.select(space, key, **kwargs)

    def insert(self, space, t, *,
               replace=False, timeout=-1, tuple_as_dict=None):
        """
            Insert request coroutine.

            Examples:

            .. code-block:: pycon

                # Basic usage
                >>> res = await conn.insert('tester', [0, 'hello'])
                >>> res
                <Response: code=0, sync=7, body_len=4>
                >>> res.body
                [[0, 'hello']]

                # Getting dict results
                >>> res = await conn.insert('tester', [0, 'hello'],
                ...                         tuple_as_dict=True)
                >>> res.body
                [{'id': 0, 'text': 'hello'}]

                # Using dict as an argument tuple
                >>> res = await conn.insert('tester', {
                ...                             'id': 0
                ...                             'text': 'hell0'
                ...                         },
                ...                         tuple_as_dict=True)
                >>> res.body
                [{'id': 0, 'text': 'hello'}]

            :param space: space id or space name.
            :param t: tuple to insert (list object)
            :param replace: performs replace request instead of insert
            :param timeout: Request timeout
            :param tuple_as_dict: Decode tuple according to schema

            :returns: :class:`asynctnt.Response` instance
        """
        return self._db.insert(space, t,
                               replace=replace,
                               timeout=timeout,
                               tuple_as_dict=tuple_as_dict)

    def replace(self, space, t, *,
                timeout=-1, tuple_as_dict=None):
        """
            Replace request coroutine. Same as insert, but replace.

            :param space: space id or space name.
            :param t: tuple to insert (list object)
            :param timeout: Request timeout
            :param tuple_as_dict: Decode tuple according to schema

            :returns: :class:`asynctnt.Response` instance
        """
        return self._db.replace(space, t,
                                timeout=timeout,
                                tuple_as_dict=tuple_as_dict)

    def delete(self, space, key, **kwargs):
        """
            Delete request coroutine.

            Examples:

            .. code-block:: pycon

                # Assuming tuple [0, 'hello'] is in space tester

                >>> res = await conn.delete('tester', [0])
                >>> res.body
                [[0, 'hello']]

                >>> res = await conn.delete('tester', [0],
                ...                         tuple_as_dict=True)
                >>> res.body
                [{'id': 0, 'text': 'hello'}]

            :param space: space id or space name.
            :param key: key to delete
            :param index: index id or name
            :param timeout: Request timeout
            :param tuple_as_dict: Decode tuple according to schema

            :returns: :class:`asynctnt.Response` instance
        """
        return self._db.delete(space, key, **kwargs)

    def update(self, space, key, operations, **kwargs):
        """
            Update request coroutine.

            Examples:

            .. code-block:: pycon

                # Assuming tuple [0, 'hello'] is in space tester

                >>> res = await conn.update('tester', [0],
                ...                         [ ['=', 1, 'hi!'] ])
                >>> res.body
                [[0, 'hi!']]

                # you can use fields names as well
                >>> res = await conn.update('tester', [0],
                ...                         [ ['=', 'text', 'hola'] ])
                >>> res.body
                [[0, 'hola']]

                # ... and retrieve tuples as dicts, of course
                >>> res = await conn.update('tester', [0],
                ...                         [ ['=', 'text', 'hola'] ],
                ...                         tuple_as_dict=True)
                >>> res.body
                [{'id': 0, 'text': 'hola'}]

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
            :param tuple_as_dict: Decode tuple according to schema

            :returns: :class:`asynctnt.Response` instance
        """
        return self._db.update(space, key, operations, **kwargs)

    def upsert(self, space, t, operations, **kwargs):
        """
            Update request coroutine. Performs either insert or update
            (depending of either tuple exists or not)

            Examples:

            .. code-block:: pycon

                # upsert does not return anything
                >>> res = await conn.upsert('tester', [0, 'hello'],
                ...                         [ ['=', 1, 'hi!'] ])
                >>> res.body
                []

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
            :param tuple_as_dict: Decode tuple according to schema.
                    Has no effect in upsert requests

            :returns: :class:`asynctnt.Response` instance
        """
        return self._db.upsert(space, t, operations, **kwargs)

    def _normalize_api(self):
        if (1, 6) <= self.version < (1, 7):
            Connection.call = Connection.call16


def _create_future(loop):
    try:
        return loop.create_future()
    except AttributeError:
        return asyncio.Future(loop=loop)


class _DbMock:
    def __getattr__(self, item):
        raise TarantoolNotConnectedError('Tarantool is not connected')


async def connect(**kwargs):
    """
        connect shorthand. See :class:`asynctnt.Connection` for kwargs details

        :return: :class:`asynctnt.Connection` object
    """
    c = Connection(**kwargs)
    await c.connect()
    return c
