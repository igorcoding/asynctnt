"""Tests for connection lifecycle."""

from __future__ import annotations

import asyncio
import uuid

import pytest

import asynctnt
from asynctnt import Response
from asynctnt.connection import ConnectionState
from asynctnt.exceptions import (
    ErrorCode,
    TarantoolDatabaseError,
    TarantoolNotConnectedError,
)
from asynctnt.instance import TarantoolSyncInstance
from tests.conftest import create_tarantool_instance, read_applua


class TestConnect:
    """Connection lifecycle tests - uses tnt fixture, not conn."""

    async def test_connect(self, tnt: TarantoolSyncInstance) -> None:
        conn = asynctnt.Connection(host=tnt.host, port=tnt.port, reconnect_timeout=0)
        assert conn.host == tnt.host
        assert conn.port == tnt.port
        assert conn.username is None
        assert conn.password is None
        assert conn.reconnect_timeout == 0
        assert conn.connect_timeout == 3
        assert conn.initial_read_buffer_size is None
        assert conn.schema_id is None
        assert conn.version is None
        assert conn.state == ConnectionState.DISCONNECTED
        assert repr(conn) == (
            f"<asynctnt.Connection host={conn.host} port={conn.port} "
            f"state={conn.state!r}>"
        )

        c = await conn.connect()
        assert c is conn
        assert conn.is_connected
        assert conn.is_fully_connected
        assert conn.state == ConnectionState.CONNECTED
        assert conn.version is not None
        assert conn.schema.id is not None
        assert conn.schema.spaces is not None

        await conn.call("box.info")
        await conn.disconnect()

    async def test_connect_direct(self, tnt: TarantoolSyncInstance) -> None:
        conn = await asynctnt.connect(host=tnt.host, port=tnt.port, reconnect_timeout=0)
        try:
            assert conn.host == tnt.host
            assert conn.port == tnt.port
            assert conn.is_connected
            assert conn.is_fully_connected
            assert conn.state == ConnectionState.CONNECTED
            assert conn.version is not None
            await conn.call("box.info")
        finally:
            await conn.disconnect()

    async def test_connect_unix(
        self, tnt: TarantoolSyncInstance, in_docker: bool
    ) -> None:
        if in_docker:
            pytest.skip("Skipping as running inside docker")

        applua = read_applua()

        unix_tnt = TarantoolSyncInstance(
            host="unix/",
            port=f"/tmp/{uuid.uuid4().hex}.sock",
            console_host="127.0.0.1",
            applua=applua,
        )
        unix_tnt.start()
        try:
            conn = await asynctnt.connect(host=unix_tnt.host, port=unix_tnt.port)
            assert conn.is_connected
            await conn.disconnect()
        finally:
            unix_tnt.stop()

    async def test_connect_contextmanager(self, tnt: TarantoolSyncInstance) -> None:
        conn = asynctnt.Connection(host=tnt.host, port=tnt.port, reconnect_timeout=0)
        assert conn.state == ConnectionState.DISCONNECTED

        async with conn:
            assert conn.is_connected
            assert conn.is_fully_connected
            assert conn.state == ConnectionState.CONNECTED
            await conn.call("box.info")

        assert conn.state == ConnectionState.DISCONNECTED

    async def test_connect_contextmanager_connect_inside(
        self, tnt: TarantoolSyncInstance
    ) -> None:
        conn = asynctnt.Connection(host=tnt.host, port=tnt.port, reconnect_timeout=0)
        async with conn:
            await conn.connect()
            assert conn.state == ConnectionState.CONNECTED
            await conn.call("box.info")
        assert conn.state == ConnectionState.DISCONNECTED

    async def test_connect_contextmanager_disconnect_inside(
        self, tnt: TarantoolSyncInstance
    ) -> None:
        conn = asynctnt.Connection(host=tnt.host, port=tnt.port, reconnect_timeout=0)
        async with conn:
            await conn.disconnect()
            assert conn.state == ConnectionState.DISCONNECTED

            with pytest.raises(TarantoolNotConnectedError):
                await conn.call("box.info")

        assert conn.state == ConnectionState.DISCONNECTED

    async def test_connect_no_schema(self, tnt: TarantoolSyncInstance) -> None:
        conn = asynctnt.Connection(
            host=tnt.host,
            port=tnt.port,
            reconnect_timeout=0,
            fetch_schema=False,
            auto_refetch_schema=False,
        )
        async with conn:
            assert conn.is_connected
            await conn.call("box.info")

    async def test_connect_auth(self, tnt: TarantoolSyncInstance) -> None:
        conn = asynctnt.Connection(
            host=tnt.host,
            port=tnt.port,
            username="t1",
            password="t1",
            reconnect_timeout=0,
        )
        async with conn:
            assert conn.is_connected
            await conn.call("box.info")

    async def test_connect_auth_no_schema(self, tnt: TarantoolSyncInstance) -> None:
        conn = asynctnt.Connection(
            host=tnt.host,
            port=tnt.port,
            username="t1",
            password="t1",
            fetch_schema=False,
            auto_refetch_schema=False,
            reconnect_timeout=0,
        )
        assert conn.username == "t1"
        assert conn.password == "t1"
        async with conn:
            assert conn.is_connected
            await conn.call("box.info")

    async def test_disconnect(self, tnt: TarantoolSyncInstance) -> None:
        conn = asynctnt.Connection(host=tnt.host, port=tnt.port, reconnect_timeout=0)
        await conn.connect()
        await conn.disconnect()
        assert not conn.is_connected
        assert not conn.is_fully_connected
        assert conn.state == ConnectionState.DISCONNECTED

        with pytest.raises(TarantoolNotConnectedError):
            await conn.call("box.info")

    async def test_disconnect_in_request(self, tnt: TarantoolSyncInstance) -> None:
        conn = asynctnt.Connection(host=tnt.host, port=tnt.port, reconnect_timeout=0)
        await conn.connect()

        coro = asyncio.ensure_future(conn.eval('require "fiber".sleep(2)'))
        await asyncio.sleep(0.5)
        await conn.disconnect()

        with pytest.raises(TarantoolNotConnectedError):
            await coro

    async def test_disconnect_auth(self, tnt: TarantoolSyncInstance) -> None:
        conn = asynctnt.Connection(
            host=tnt.host,
            port=tnt.port,
            username="t1",
            password="t1",
            reconnect_timeout=0,
        )
        await conn.connect()
        await conn.disconnect()
        assert not conn.is_connected
        assert conn.state == ConnectionState.DISCONNECTED

        with pytest.raises(TarantoolNotConnectedError):
            await conn.call("box.info")

    async def test_disconnect_while_reconnecting(
        self, tnt: TarantoolSyncInstance
    ) -> None:
        conn = asynctnt.Connection(
            host=tnt.host,
            port=tnt.port,
            username="t1",
            password="t1",
            reconnect_timeout=0.1,
        )
        assert conn.reconnect_timeout == 0.1
        try:
            await conn.connect()
            tnt.stop()
            await asyncio.sleep(0.5)

            await conn.disconnect()

            assert not conn.is_connected
            assert conn.state == ConnectionState.DISCONNECTED

            with pytest.raises(TarantoolNotConnectedError):
                await conn.call("box.info")
        finally:
            tnt.start()

    async def test_close_while_reconnecting(self, tnt: TarantoolSyncInstance) -> None:
        conn = asynctnt.Connection(
            host=tnt.host,
            port=tnt.port,
            username="t1",
            password="t1",
            reconnect_timeout=0.1,
        )
        try:
            await conn.connect()
            tnt.stop()
            await asyncio.sleep(0.5)

            conn.close()

            assert not conn.is_connected
            assert conn.state == ConnectionState.DISCONNECTED

            with pytest.raises(TarantoolNotConnectedError):
                await conn.call("box.info")
        finally:
            tnt.start()

    async def test_connect_multiple(self, tnt: TarantoolSyncInstance) -> None:
        conn = asynctnt.Connection(
            host=tnt.host,
            port=tnt.port,
            fetch_schema=False,
            reconnect_timeout=0,
        )
        for _ in range(10):
            await conn.connect()
            await conn.disconnect()
        assert not conn.is_connected
        assert conn.state == ConnectionState.DISCONNECTED

        with pytest.raises(TarantoolNotConnectedError):
            await conn.call("box.info")

    async def test_connect_cancel(self, tnt: TarantoolSyncInstance) -> None:
        conn = asynctnt.Connection(
            host=tnt.host,
            port=tnt.port,
            fetch_schema=True,
            reconnect_timeout=0,
        )
        try:
            f = asyncio.ensure_future(conn.connect())
            await asyncio.sleep(0.0001)
            f.cancel()
            with pytest.raises(asyncio.CancelledError):
                await f
        finally:
            await conn.disconnect()

    async def test_connect_error_no_reconnect(self) -> None:
        conn = asynctnt.Connection(
            host="127.0.0.1", port=1, fetch_schema=True, reconnect_timeout=0
        )
        with pytest.raises(ConnectionRefusedError):
            await conn.connect()

    async def test_connect_tnt_restarted(self, tnt: TarantoolSyncInstance) -> None:
        conn = asynctnt.Connection(
            host=tnt.host,
            port=tnt.port,
            username="t1",
            password="t1",
            fetch_schema=True,
            reconnect_timeout=0.000001,
        )
        await conn.connect()

        try:
            tnt.stop()
            tnt.start()
            await asyncio.sleep(0.5)
            await conn.ping()
        finally:
            await conn.disconnect()

    async def test_connect_force_disconnect(self, tnt: TarantoolSyncInstance) -> None:
        conn = asynctnt.Connection(host=tnt.host, port=44444, reconnect_timeout=0.3)
        asyncio.ensure_future(conn.connect())
        await asyncio.sleep(1)
        await conn.disconnect()
        assert conn.state == ConnectionState.DISCONNECTED

    async def test_close(self, tnt: TarantoolSyncInstance) -> None:
        conn = asynctnt.Connection(host=tnt.host, port=tnt.port, reconnect_timeout=0)
        await conn.connect()
        await asyncio.sleep(0.1)
        conn.close()
        await asyncio.sleep(0.1)
        assert conn.state == ConnectionState.DISCONNECTED

    async def test_disconnect_from_idle(self, tnt: TarantoolSyncInstance) -> None:
        conn = asynctnt.Connection(host=tnt.host, port=tnt.port, reconnect_timeout=0)
        await conn.disconnect()
        assert conn.state == ConnectionState.DISCONNECTED

    async def test_reconnect_from_idle(self, tnt: TarantoolSyncInstance) -> None:
        conn = asynctnt.Connection(host=tnt.host, port=tnt.port, reconnect_timeout=0)
        await conn.reconnect()
        try:
            assert conn.state == ConnectionState.CONNECTED
            await conn.call("box.info")
        finally:
            await conn.disconnect()

    async def test_reconnect_after_connect(self, tnt: TarantoolSyncInstance) -> None:
        conn = asynctnt.Connection(host=tnt.host, port=tnt.port, reconnect_timeout=0)
        try:
            await conn.connect()
            await conn.reconnect()

            assert conn.state == ConnectionState.CONNECTED
            await conn.call("box.info")
        finally:
            await conn.disconnect()

    async def test_manual_reconnect(self, tnt: TarantoolSyncInstance) -> None:
        conn = asynctnt.Connection(host=tnt.host, port=tnt.port, reconnect_timeout=0)
        try:
            await conn.connect()
            await conn.disconnect()
            await conn.connect()

            assert conn.state == ConnectionState.CONNECTED
            await conn.call("box.info")
        finally:
            await conn.disconnect()

    async def test_connect_connection_lost(self, tnt: TarantoolSyncInstance) -> None:
        conn = asynctnt.Connection(
            host=tnt.host, port=tnt.port, reconnect_timeout=1 / 3
        )
        try:
            await conn.connect()
            tnt.stop()
            await asyncio.sleep(0.5)
            tnt.start()
            await asyncio.sleep(0.5)

            assert conn.state == ConnectionState.CONNECTED
            assert conn.is_connected
            await conn.call("box.info")
        finally:
            await conn.disconnect()

    async def test_connect_from_multiple_coroutines(
        self, tnt: TarantoolSyncInstance
    ) -> None:
        conn = asynctnt.Connection(
            host=tnt.host, port=tnt.port, reconnect_timeout=1 / 3
        )
        try:
            coros = []
            for _ in range(10):
                coros.append(asyncio.ensure_future(conn.connect()))

            await asyncio.gather(*coros)
            assert conn.state == ConnectionState.CONNECTED
            assert conn.is_connected
            await conn.call("box.info")
        finally:
            await conn.disconnect()

    async def test_disconnect_from_multiple_coroutines(
        self, tnt: TarantoolSyncInstance
    ) -> None:
        conn = asynctnt.Connection(
            host=tnt.host, port=tnt.port, reconnect_timeout=1 / 3
        )
        try:
            await conn.connect()
            coros = []
            for _ in range(10):
                coros.append(asyncio.ensure_future(conn.disconnect()))

            await asyncio.gather(*coros)
            assert conn.state == ConnectionState.DISCONNECTED
            assert not conn.is_connected

            with pytest.raises(TarantoolNotConnectedError):
                await conn.call("box.info")
        finally:
            await conn.disconnect()

    async def test_connect_while_reconnecting(self, tnt: TarantoolSyncInstance) -> None:
        conn = asynctnt.Connection(host=tnt.host, port=tnt.port, reconnect_timeout=1)

        try:
            coros = []
            for _ in range(10):
                coros.append(asyncio.ensure_future(conn.connect()))

            tnt.stop()
            await asyncio.sleep(0.5)

            connect_coros = asyncio.ensure_future(asyncio.gather(*coros))

            tnt.start()
            await asyncio.sleep(1)
            await connect_coros

            assert conn.state == ConnectionState.CONNECTED
            assert conn.is_connected

            await conn.call("box.info")
        finally:
            await conn.disconnect()

    async def test_connect_on_tnt_crash_no_reconnect(
        self, tnt: TarantoolSyncInstance
    ) -> None:
        conn = asynctnt.Connection(
            host=tnt.host,
            port=tnt.port,
            connect_timeout=1,
            reconnect_timeout=0,
        )
        try:
            await conn.connect()
            await asyncio.sleep(0.5)
            try:
                await conn.eval("require('ffi').cast('char *', 0)[0] = 48")
            except TarantoolNotConnectedError:
                pass  # Expected
            tnt.stop()
            tnt.start()
            await asyncio.sleep(1)
            await conn.connect()  # this connect should reconnect easily

            await conn.call("box.info")
        finally:
            await conn.disconnect()

    async def test_connect_on_tnt_crash_with_reconnect(
        self, tnt: TarantoolSyncInstance
    ) -> None:
        conn = asynctnt.Connection(
            host=tnt.host,
            port=tnt.port,
            connect_timeout=1,
            reconnect_timeout=1 / 3,
        )
        try:
            await conn.connect()
            await asyncio.sleep(0.5)
            try:
                await conn.eval("require('ffi').cast('char *', 0)[0] = 48")
            except TarantoolNotConnectedError:
                pass  # Expected
            tnt.stop()
            tnt.start()
            await asyncio.sleep(1)
            await conn.connect()  # this connect should reconnect easily

            await conn.call("box.info")
        finally:
            await conn.disconnect()

    async def test_connect_wait_tnt_started(self, tnt: TarantoolSyncInstance) -> None:
        tnt.stop()
        conn = asynctnt.Connection(
            host=tnt.host,
            port=tnt.port,
            username="t1",
            password="t1",
            fetch_schema=True,
            reconnect_timeout=0.000000001,
        )
        try:
            coro = asyncio.ensure_future(conn.connect())
            await asyncio.sleep(0.3)
            tnt.start()
            await asyncio.sleep(1)
            while True:
                try:
                    await coro
                    break
                except TarantoolDatabaseError as e:
                    if e.code == ErrorCode.ER_NO_SUCH_USER:
                        # Try again
                        coro = asyncio.ensure_future(conn.connect())
                        continue
                    raise

            assert conn.state == ConnectionState.CONNECTED
            await conn.call("box.info")
        finally:
            await conn.disconnect()

    async def test_connect_waiting_for_spaces(
        self, tnt: TarantoolSyncInstance, in_docker: bool
    ) -> None:
        if in_docker:
            pytest.skip("not running in docker")

        with create_tarantool_instance(replication_source=["x:1"]) as instance:
            instance.start(wait=False)

            conn = asynctnt.Connection(
                host=instance.host,
                port=instance.port,
                fetch_schema=True,
                reconnect_timeout=0.1,
                connect_timeout=10,
            )
            assert conn.connect_timeout == 10
            try:
                states: dict[ConnectionState, bool] = {}

                async def state_checker() -> None:
                    while True:
                        states[conn.state] = True
                        await asyncio.sleep(0.001)

                checker = asyncio.ensure_future(state_checker())

                try:
                    await asyncio.wait_for(conn.connect(), 1)
                except asyncio.TimeoutError:
                    pass  # connect cancelled as expected

                checker.cancel()

                assert states.get(ConnectionState.CONNECTING, False), "was in connecting"

                with pytest.raises(TarantoolNotConnectedError):
                    await conn.call("box.info")
            finally:
                await conn.disconnect()

    @pytest.mark.min_bin_version((1, 7))
    async def test_connect_waiting_for_spaces_no_reconnect(
        self, tnt: TarantoolSyncInstance, in_docker: bool
    ) -> None:
        if in_docker:
            pytest.skip("not running in docker")

        with create_tarantool_instance(replication_source=["x:1"]) as instance:
            instance.start(wait=False)
            await asyncio.sleep(1)

            conn = asynctnt.Connection(
                host=instance.host,
                port=instance.port,
                fetch_schema=True,
                reconnect_timeout=0,
                connect_timeout=10,
            )
            try:
                with pytest.raises(TarantoolDatabaseError) as exc:
                    await conn.connect()

                assert exc.value.code == ErrorCode.ER_NO_SUCH_SPACE
            finally:
                await conn.disconnect()

    async def test_connect_waiting_for_spaces_no_reconnect_1_6(
        self, tnt: TarantoolSyncInstance, in_docker: bool
    ) -> None:
        if in_docker:
            pytest.skip("not running in docker")

        with create_tarantool_instance(replication_source=["x:1"]) as instance:
            instance.start(wait=False)
            await asyncio.sleep(1)

            # Check if version < 1.7
            async with asynctnt.Connection(
                host=instance.host, port=instance.port, fetch_schema=False
            ) as check_conn:
                if check_conn.version >= (1, 7):
                    pytest.skip("Test only for Tarantool < 1.7")

            conn = asynctnt.Connection(
                host=instance.host,
                port=instance.port,
                fetch_schema=True,
                reconnect_timeout=0,
                connect_timeout=10,
            )
            try:
                with pytest.raises(ConnectionRefusedError):
                    await conn.connect()
            finally:
                await conn.disconnect()

    @pytest.mark.min_bin_version((1, 7))
    async def test_connect_err_loading(
        self, tnt: TarantoolSyncInstance, in_docker: bool
    ) -> None:
        if in_docker:
            pytest.skip("not running in docker")

        with create_tarantool_instance(replication_source=["x:1"]) as instance:
            instance.start(wait=False)
            await asyncio.sleep(1)

            conn = asynctnt.Connection(
                host=instance.host,
                port=instance.port,
                username="t1",
                password="t1",
                fetch_schema=True,
                reconnect_timeout=0,
                connect_timeout=10,
            )
            try:
                with pytest.raises(TarantoolDatabaseError) as exc:
                    await conn.connect()

                assert exc.value.code == ErrorCode.ER_LOADING
            finally:
                await conn.disconnect()

    async def test_connect_err_loading_1_6(
        self, tnt: TarantoolSyncInstance, in_docker: bool
    ) -> None:
        if in_docker:
            pytest.skip("not running in docker")

        with create_tarantool_instance(replication_source=["x:1"]) as instance:
            instance.start(wait=False)
            await asyncio.sleep(1)

            # Check if version < 1.7
            async with asynctnt.Connection(
                host=instance.host, port=instance.port, fetch_schema=False
            ) as check_conn:
                if check_conn.version >= (1, 7):
                    pytest.skip("Test only for Tarantool < 1.7")

            conn = asynctnt.Connection(
                host=instance.host,
                port=instance.port,
                username="t1",
                password="t1",
                fetch_schema=True,
                reconnect_timeout=0,
                connect_timeout=10,
            )
            try:
                with pytest.raises(ConnectionRefusedError):
                    await conn.connect()
            finally:
                await conn.disconnect()

    async def test_connect_invalid_user_no_reconnect(
        self, tnt: TarantoolSyncInstance
    ) -> None:
        async with asynctnt.Connection(host=tnt.host, port=tnt.port) as check_conn:
            version = check_conn.version

        conn = asynctnt.Connection(
            host=tnt.host,
            port=tnt.port,
            username="fancy",
            password="man",
            connect_timeout=1,
            reconnect_timeout=0,
        )
        with pytest.raises(TarantoolDatabaseError) as exc:
            await conn.connect()

        err_code = ErrorCode.ER_PASSWORD_MISMATCH
        if version < (2, 11):
            err_code = ErrorCode.ER_NO_SUCH_USER
        assert exc.value.code == err_code

    async def test_connect_invalid_user_with_reconnect(
        self, tnt: TarantoolSyncInstance
    ) -> None:
        conn = asynctnt.Connection(
            host=tnt.host,
            port=tnt.port,
            fetch_schema=True,
            reconnect_timeout=0.1,
            connect_timeout=10,
        )
        await conn.connect()  # first connect successfully

        # then change credentials
        conn._username = "fancy"
        conn._password = "man"

        tnt.stop()
        tnt.start()
        await asyncio.sleep(0.1)
        try:
            states: dict[ConnectionState, bool] = {}

            async def state_checker() -> None:
                while True:
                    states[conn.state] = True
                    await asyncio.sleep(0.001)

            checker = asyncio.ensure_future(state_checker())

            try:
                await asyncio.wait_for(conn.connect(), 1)
            except asyncio.TimeoutError:
                pass  # connect cancelled as expected

            checker.cancel()

            assert states.get(ConnectionState.CONNECTING, False), "was in connecting"
            assert states.get(
                ConnectionState.RECONNECTING, False
            ), "was in reconnecting"

            with pytest.raises(TarantoolNotConnectedError):
                await conn.call("box.info")
        finally:
            await conn.disconnect()

    @pytest.mark.min_bin_version((2, 10))
    async def test_features(self, tnt: TarantoolSyncInstance) -> None:
        async with asynctnt.Connection(host=tnt.host, port=tnt.port) as conn:
            if conn.version >= (3, 0):
                pytest.skip(f"Requires Tarantool < (3, 0), got {conn.version}")

            assert conn.features is not None
            assert conn.features.streams
            assert conn.features.watchers
            assert conn.features.error_extension
            assert conn.features.transactions
            assert conn.features.pagination

            assert not conn.features.space_and_index_names
            assert not conn.features.watch_once
            assert not conn.features.dml_tuple_extension
            assert not conn.features.call_ret_tuple_extension
            assert not conn.features.call_arg_tuple_extension

    @pytest.mark.min_bin_version((3, 0))
    async def test_features_3_0(self, tnt: TarantoolSyncInstance) -> None:
        async with asynctnt.Connection(host=tnt.host, port=tnt.port) as conn:
            assert conn.features is not None
            assert conn.features.streams
            assert conn.features.watchers
            assert conn.features.error_extension
            assert conn.features.transactions
            assert conn.features.pagination

            assert conn.features.space_and_index_names
            assert conn.features.watch_once
            assert conn.features.dml_tuple_extension
            assert conn.features.call_ret_tuple_extension
            assert conn.features.call_arg_tuple_extension

    async def test_connect_parallel(self, tnt: TarantoolSyncInstance) -> None:
        n = 10

        async def create_connection() -> None:
            conn = asynctnt.Connection(
                host=tnt.host,
                port=tnt.port,
            )
            await conn.connect()
            res = await conn.ping()
            assert isinstance(res, Response)
            await conn.disconnect()

        await asyncio.gather(*[create_connection() for _ in range(n)])

    async def test_connect_parallel_all_at_once(
        self, tnt: TarantoolSyncInstance
    ) -> None:
        n = 10

        conns = []
        for _ in range(n):
            conn = asynctnt.Connection(
                host=tnt.host,
                port=tnt.port,
            )
            conns.append(conn)

        await asyncio.gather(*[conn.connect() for conn in conns])

        res = await asyncio.gather(*[conn.ping() for conn in conns])
        for r in res:
            assert isinstance(r, Response)

        await asyncio.gather(*[conn.disconnect() for conn in conns])
