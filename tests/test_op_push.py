"""Tests for push operation."""

from __future__ import annotations

import asyncio

import pytest

import asynctnt
from asynctnt import PushIterator, Response
from asynctnt.exceptions import TarantoolDatabaseError, TarantoolNotConnectedError
from asynctnt.instance import TarantoolSyncInstance
from tests.conftest import ensure_version


class TestPush:
    """Push operation tests."""

    @ensure_version(min=(1, 10))
    async def test_push_invalid_future(self, conn: asynctnt.Connection) -> None:
        with pytest.raises(ValueError) as e:
            PushIterator(asyncio.Future())

        assert str(e.value) == (
            "Future is invalid. Make sure to call with "
            "a future returned from a method with "
            "push_subscribe=True flag"
        )

    @ensure_version(min=(1, 10))
    async def test_push_invalid_future_no_flag(self, conn: asynctnt.Connection) -> None:
        res = conn.call("async_action")

        with pytest.raises(ValueError) as e:
            PushIterator(res)

        assert str(e.value) == (
            "Future is invalid. Make sure to call with "
            "a future returned from a method with "
            "push_subscribe=True flag"
        )

    @ensure_version(min=(1, 10))
    async def test_push_correct_res(self, conn: asynctnt.Connection) -> None:
        fut = conn.call("async_action", push_subscribe=True)
        assert type(fut) is asyncio.Future

        # Should not raise
        it = PushIterator(fut)
        assert isinstance(it.response, asynctnt.Response)

    @ensure_version(min=(1, 10))
    async def test_push_call_iter(self, conn: asynctnt.Connection) -> None:
        fut = conn.call("async_action", push_subscribe=True)

        with pytest.raises(RuntimeError) as e:
            for _ in PushIterator(fut):
                pass
        assert str(e.value) == "Cannot use iter with PushIterator - use aiter"

    @ensure_version(min=(1, 10))
    async def test_push_read_all(self, conn: asynctnt.Connection) -> None:
        fut = conn.call("async_action", push_subscribe=True)
        it = PushIterator(fut)

        assert not it.response.done(), "response not done"

        result = []

        async for entry in it:
            result.append(entry[0])

        assert it.response.done(), "response is done"

        assert result == ["hello_1", "hello_2", "hello_3", "hello_4", "hello_5"], (
            "push values ok"
        )

        fut_res = await fut
        assert isinstance(fut_res, Response), "got response"
        assert fut_res.code == 0, "code ok"
        assert fut_res.sync == it.response.sync, "sync ok"
        assert fut_res.return_code == 0, "return code ok"
        assert fut_res.body == ["ret"], "return value ok"
        assert fut_res.done(), "response done"

    @ensure_version(min=(1, 10))
    async def test_push_read_in_parts(self, conn: asynctnt.Connection) -> None:
        fut = conn.call("async_action", push_subscribe=True)
        it = PushIterator(fut)

        result = []

        i = 0
        async for entry in it:
            if len(entry) == 0:
                pytest.fail(f"got 0 length for entry #{i}")
            result.append(entry[0])
            i += 1
            if i == 2:
                break

        async for entry in it:
            if len(entry) == 0:
                pytest.fail(f"got 0 length for entry #{i}")
            result.append(entry[0])
            i += 1

        assert result == ["hello_1", "hello_2", "hello_3", "hello_4", "hello_5"], (
            "push values ok"
        )

        fut_res = await fut
        assert isinstance(fut_res, Response), "got response"
        assert fut_res.code == 0, "code ok"
        assert fut_res.sync == it.response.sync, "sync ok"
        assert fut_res.return_code == 0, "return code ok"
        assert fut_res.body == ["ret"], "return value ok"

    @ensure_version(min=(1, 10))
    async def test_push_read_all_eval(self, conn: asynctnt.Connection) -> None:
        fut = conn.eval(
            """
            for i = 1, 5 do
                box.session.push('hello_' .. tostring(i))
                require'fiber'.sleep(0.01)
            end
            return 'ret'
        """,
            push_subscribe=True,
        )
        it = PushIterator(fut)

        result = []

        i = 0
        async for entry in it:
            if len(entry) == 0:
                pytest.fail(f"got 0 length for entry #{i}")
            result.append(entry[0])
            i += 1

        assert result == ["hello_1", "hello_2", "hello_3", "hello_4", "hello_5"], (
            "push values ok"
        )

        fut_res = await fut
        assert isinstance(fut_res, Response), "got response"
        assert fut_res.code == 0, "code ok"
        assert fut_res.sync == it.response.sync, "sync ok"
        assert fut_res.return_code == 0, "return code ok"
        assert fut_res.body == ["ret"], "return value ok"

    @ensure_version(min=(1, 10))
    async def test_push_read_all_various_sleep(self, conn: asynctnt.Connection) -> None:
        fut = conn.eval(
            """
            box.session.push('hello_1')
            require'fiber'.sleep(0.01)
            box.session.push('hello_2')
            require'fiber'.sleep(1)
            box.session.push('hello_3')
            return 'ret'
        """,
            push_subscribe=True,
        )
        it = PushIterator(fut)

        result = []

        async for entry in it:
            result.append(entry[0])

        assert result == ["hello_1", "hello_2", "hello_3"], "push values ok"

        fut_res = await fut
        assert isinstance(fut_res, Response), "got response"
        assert fut_res.code == 0, "code ok"
        assert fut_res.sync == it.response.sync, "sync ok"
        assert fut_res.return_code == 0, "return code ok"
        assert fut_res.body == ["ret"], "return value ok"

    @ensure_version(min=(1, 10))
    async def test_push_read_all_error(
        self, conn: asynctnt.Connection, tnt: TarantoolSyncInstance
    ) -> None:
        fut = conn.eval(
            """
            for i = 1, 5 do
                box.session.push('hello_' .. tostring(i))
                require'fiber'.sleep(0.01)
            end
            return 'ret'
        """,
            push_subscribe=True,
        )
        it = PushIterator(fut)

        # iter once
        await it.__anext__()

        # drop tarantool
        tnt.stop()

        try:
            with pytest.raises(TarantoolNotConnectedError):
                await asyncio.wait_for(it.__anext__(), timeout=5)
        finally:
            tnt.start()

    @ensure_version(min=(1, 10))
    async def test_push_read_all_disconnect(self, conn: asynctnt.Connection) -> None:
        fut = conn.eval("error('some error')", push_subscribe=True)
        it = PushIterator(fut)

        with pytest.raises(TarantoolDatabaseError):
            await it.__anext__()

        with pytest.raises(TarantoolDatabaseError):
            await fut

    @ensure_version(min=(1, 10))
    async def test_push_read_all_multiple_iterators(
        self, conn: asynctnt.Connection
    ) -> None:
        fut = conn.eval(
            "box.session.push(1);box.session.push(2);box.session.push(3);",
            push_subscribe=True,
        )
        it1 = PushIterator(fut)
        it2 = PushIterator(fut)

        async def f(it: PushIterator) -> list:
            results = []
            async for entry in it:
                results.append(entry[0])
            return results

        res1, res2 = await asyncio.gather(f(it1), f(it2))
        res1.extend(res2)
        res1.sort()

        assert res1 == [1, 2, 3]

    @ensure_version(min=(1, 10))
    async def test_push_read_all_one_iterator(self, conn: asynctnt.Connection) -> None:
        fut = conn.eval(
            "box.session.push('hello_1');"
            "box.session.push('hello_2');"
            "box.session.push('hello_3');",
            push_subscribe=True,
        )

        it = PushIterator(fut)
        results = []
        async for entry in it:
            results.append(entry[0])

        assert results == ["hello_1", "hello_2", "hello_3"], "push ok"
