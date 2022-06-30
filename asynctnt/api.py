import enum
from typing import Optional, List, Any, Union, Dict

from asynctnt.iproto import protocol

from .exceptions import TarantoolNotConnectedError
from .prepared import PreparedStatement
from .types import MethodRet, SpaceType, KeyType, TupleType


class _DbMock:
    def __getattr__(self, item):
        raise TarantoolNotConnectedError('Tarantool is not connected')


class Isolation(enum.IntEnum):
    DEFAULT = 0
    READ_COMMITTED = 1
    READ_CONFIRMED = 2
    BEST_EFFORT = 3


class Api:
    __slots__ = (
        '_db',
    )

    def __init__(self):
        self._db: Union[_DbMock, protocol.Db] = _DbMock()

    def _set_db(self, db: protocol.Db):
        self._db = db

    def _clear_db(self):
        self._db = _DbMock()

    def ping(self, *, timeout: float = -1.0) -> MethodRet:
        """
            Ping request coroutine

            :param timeout: Request timeout

            :returns: :class:`asynctnt.Response` instance
        """
        return self._db.ping(timeout=timeout)

    def call16(self,
               func_name: str,
               args: Optional[List[Any]] = None,
               *,
               timeout: float = -1.0,
               push_subscribe: bool = False) -> MethodRet:
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

    def call(self,
             func_name: str,
             args: Optional[List[Any]] = None,
             *,
             timeout: float = -1.0,
             push_subscribe: bool = False) -> MethodRet:
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

    def eval(self,
             expression: str,
             args: Optional[List[Any]] = None,
             *,
             timeout: float = -1.0,
             push_subscribe: bool = False) -> MethodRet:
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

    def select(self,
               space: SpaceType,
               key: Optional[KeyType] = None,
               **kwargs) -> MethodRet:
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

    def insert(self,
               space: SpaceType,
               t: TupleType,
               *,
               replace: bool = False,
               timeout: float = -1) -> MethodRet:
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

    def replace(self,
                space: SpaceType,
                t: TupleType,
                *,
                timeout: float = -1.0) -> MethodRet:
        """
            Replace request coroutine. Same as insert, but replace.

            :param space: space id or space name.
            :param t: tuple to insert (list object)
            :param timeout: Request timeout

            :returns: :class:`asynctnt.Response` instance
        """
        return self._db.replace(space, t, timeout=timeout)

    def delete(self,
               space: SpaceType,
               key: KeyType,
               **kwargs) -> MethodRet:
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

    def update(self,
               space: SpaceType,
               key: KeyType,
               operations: List[Any],
               **kwargs) -> MethodRet:
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

    def upsert(self,
               space: SpaceType,
               t: TupleType,
               operations: List[Any],
               **kwargs) -> MethodRet:
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

    def execute(self,
                query: Union[str, int],
                args: Optional[List[Union[Dict[str, Any], Any]]] = None, *,
                parse_metadata: bool = True,
                timeout: float = -1.0) -> MethodRet:
        """
            Executes an SQL statement (only for Tarantool > 2)

            Examples:

            .. code-block:: pycon

                >>> await conn.execute("select 1 as a, 2 as b")
                <Response sync=3 rowcount=1 data=[<TarantoolTuple A=1 B=2>]>

                >>> await conn.execute("select * from sql_space")
                <Response sync=3 rowcount=2 data=[
                    <TarantoolTuple ID=1 NAME='James Bond'>,
                    <TarantoolTuple ID=2 NAME='Ethan Hunt'>
                ]>

                >>> await conn.execute("select * from sql_space",
                ...                    parse_metadata=False)
                <Response sync=3 rowcount=2 data=[
                    <TarantoolTuple 0=1 1='James Bond'>,
                    <TarantoolTuple 0=2 1='Ethan Hunt'>
                ]>

            :param query: SQL query or statement_id
            :param args: Query arguments
            :param parse_metadata: Set to False to disable response's metadata
                                   parsing for better performance
            :param timeout: Request timeout

            :returns: :class:`asynctnt.Response` instance
        """
        return self._db.execute(query, args,
                                parse_metadata=parse_metadata,
                                timeout=timeout)

    def prepare(self, query: str) -> PreparedStatement:
        """
            Create a :class:`asynctnt.prepared.PreparedStatement` instance
            :param query: query to be prepared
        """
        return PreparedStatement(self, query)

    def prepare_iproto(self,
                       query: str,
                       timeout: float = -1.0) -> MethodRet:
        """
            Low-level prepare() call
            :param query: query to be prepared
            :param timeout: request timeout
        """
        return self._db.prepare(query, timeout=timeout)

    def unprepare_iproto(self,
                         stmt_id: int,
                         timeout: float = -1.0) -> MethodRet:
        """
            Low-level unprepare() call
            :param stmt_id: query to be unprepared
            :param timeout: request timeout
        """
        return self._db.prepare(stmt_id, timeout=timeout)

    def begin(self,
              isolation: Isolation = Isolation.DEFAULT,
              tx_timeout: float = 0.0,
              timeout: float = -1.0) -> MethodRet:
        """
        Begin an interactive transaction within a stream
        :param isolation: isolation level
        :param tx_timeout: transaction timeout
        :param timeout: request timeout
        :return:
        """
        return self._db.begin(isolation.value, tx_timeout, timeout)

    def commit(self, timeout: float = -1.0) -> MethodRet:
        """
        Commit a running transaction
        :param timeout: request timeout
        """
        return self._db.commit(timeout)

    def rollback(self, timeout: float = -1.0) -> MethodRet:
        """
        Rollback a running transaction
        :param timeout: request timeout
        """
        return self._db.rollback(timeout)
