"""Tests for MessagePack extension types."""

from __future__ import annotations

import datetime
import uuid
from decimal import Decimal

import pytest
import pytz

import asynctnt
from asynctnt import IProtoError
from asynctnt.exceptions import ErrorCode, TarantoolDatabaseError
from tests.conftest import ensure_version


class TestMpExtDecimal:
    """Decimal extension type tests."""

    @pytest.mark.parametrize(
        "case",
        [
            "-12.34",
            "-12.345",
            "-12.4",
            "0.000",
            "42",
            "0.33",
            "0.000000000000000000000000000000000010",
            "-0.000000000000000000000000000000000010",
            "0.1111111111111111",
            "-0.1111111111111111",
            "-0.111111",
            "-18.34",
            "-108.123456789",
            "100",
            "0.1",
            "-0.1",
            "2.718281828459045",
            "-2.718281828459045",
            "3.141592653589793",
            "-3.141592653589793",
            "1",
            "-1",
            "0",
            "-0",
            "0.01",
            "-0.01",
            "0.001",
            "11111111111111111111111111111111111111",
            "-11111111111111111111111111111111111111",
            "0.0000000000000000000000000000000000001",
            "-0.0000000000000000000000000000000000001",
            "0.00000000000000000000000000000000000009",
            "-0.00000000000000000000000000000000000009",
            "99999999999999999999999999999999999999",
            "-99999999999999999999999999999999999999",
            "1234567891234567890.0987654321987654321",
            "-1234567891234567890.0987654321987654321",
            "1e33",
            "1.2345e33",
            "1.2345e2",
            "1.2345e4",
            "-1e33",
            "1e-33",
        ],
    )
    @ensure_version(min=(2, 2))
    async def test_decimal(self, conn: asynctnt.Connection, case: str) -> None:
        space = "tester_ext_dec"

        dec = Decimal(case)
        res = await conn.replace(space, [1, dec])
        assert res[0][1] == dec, "self-return works"

        res = await conn.eval(
            f"local decimal = require('decimal'); return decimal.new('{case}')"
        )
        assert res[0] == dec, "matches tarantool decimal"


class TestMpExtUUID:
    """UUID extension type tests."""

    @ensure_version(min=(2, 4, 1))
    async def test_uuid(self, conn: asynctnt.Connection) -> None:
        space = "tester_ext_uuid"

        val = uuid.uuid4()
        res = await conn.replace(space, [1, val])
        assert res[0][1] == val

        val = uuid.UUID("f6423bdf-b49e-4913-b361-0740c9702e4b")
        res = await conn.replace(space, [1, val])
        assert res[0][1] == val

        val = uuid.UUID("00000000-0000-0000-0000-000000000000")
        res = await conn.replace(space, [1, val])
        assert res[0][1] == val

        val = uuid.uuid1(1, 100)
        res = await conn.replace(space, [1, val])
        assert res[0][1] == val

        val = uuid.uuid3(uuid.uuid4(), "hellothere")
        res = await conn.replace(space, [1, val])
        assert res[0][1] == val

        val = uuid.uuid5(uuid.NAMESPACE_URL, "generalkenobi")
        res = await conn.replace(space, [1, val])
        assert res[0][1] == val


class TestMpExtError:
    """Error extension type tests."""

    @ensure_version(min=(2, 4, 1))
    async def test_ext_error(self, conn: asynctnt.Connection) -> None:
        try:
            await conn.eval("""
                box.schema.space.create('_space')
            """)
        except TarantoolDatabaseError as e:
            assert e.error is not None
            assert len(e.error.trace) > 0
            frame = e.error.trace[0]
            assert frame.error_type == "ClientError"
            assert frame.file is not None
            assert frame.line is not None
            assert frame.message == "Space '_space' already exists"
            assert frame.err_no == 0
            assert frame.code == ErrorCode.ER_SPACE_EXISTS

    @ensure_version(min=(2, 4, 1))
    async def test_ext_error_custom(self, conn: asynctnt.Connection) -> None:
        try:
            await conn.eval("""
                local e = box.error.new{code=5,reason='A',type='B'}
                box.error(e)
            """)
        except TarantoolDatabaseError as e:
            assert e.error is not None
            assert len(e.error.trace) > 0
            frame = e.error.trace[0]
            assert frame.error_type == "CustomError"
            assert frame.file is not None
            assert frame.line is not None
            assert frame.message == "A"
            assert frame.err_no == 0
            assert frame.code == 5
            assert "custom_type" in frame.fields
            assert frame.fields["custom_type"] == "B"

    @ensure_version(min=(2, 10))
    async def test_ext_error_custom_return(self, conn: asynctnt.Connection) -> None:
        resp = await conn.eval("""
            local e = box.error.new{code=5,reason='A',type='B'}
            return e
        """)
        e = resp[0]
        assert isinstance(e, IProtoError)
        assert len(e.trace) > 0
        frame = e.trace[0]
        assert frame.error_type == "CustomError"
        assert frame.file == "eval"
        assert frame.line == 2
        assert frame.message == "A"
        assert frame.err_no == 0
        assert frame.code == 5
        assert "custom_type" in frame.fields
        assert frame.fields["custom_type"] == "B"

    @ensure_version(min=(2, 10))
    async def test_ext_error_custom_return_with_disabled_exterror(
        self, conn: asynctnt.Connection
    ) -> None:
        await conn.eval("""
            require('msgpack').cfg{encode_error_as_ext = false}
        """)
        try:
            resp = await conn.eval("""
                local e = box.error.new{code=5,reason='A',type='B'}
                return e
            """)
            e = resp[0]
            assert isinstance(e, str)
            assert e == "A"
        finally:
            await conn.eval("""
                require('msgpack').cfg{encode_error_as_ext = true}
            """)


class TestMpExtDatetime:
    """Datetime extension type tests."""

    @ensure_version(min=(2, 10))
    async def test_ext_datetime_read(self, conn: asynctnt.Connection) -> None:
        resp = await conn.eval("""
            local date = require('datetime')
            return date.parse('2000-01-01T02:00:00.23+0300')
        """)
        res = resp[0]
        dt = datetime.datetime.fromisoformat("2000-01-01T02:00:00.230000+03:00")
        assert dt == res

    @ensure_version(min=(2, 10))
    async def test_ext_datetime_tz(self, conn: asynctnt.Connection) -> None:
        resp = await conn.eval("""
            local date = require('datetime')
            return date.parse('2000-01-01T02:00:00 MSK')
        """)
        res = resp[0]
        dt = datetime.datetime.fromisoformat("2000-01-01T02:00:00+03:00")
        assert dt == res

    @ensure_version(min=(2, 10))
    async def test_ext_datetime_read_neg_tz(self, conn: asynctnt.Connection) -> None:
        resp = await conn.eval("""
            local date = require('datetime')
            return date.parse('2000-01-01T02:17:43.23-08:00')
        """)
        res = resp[0]
        dt = datetime.datetime.fromisoformat("2000-01-01T02:17:43.230000-08:00")
        assert dt == res

    @ensure_version(min=(2, 10))
    async def test_ext_datetime_read_before_1970(
        self, conn: asynctnt.Connection
    ) -> None:
        resp = await conn.eval("""
            local date = require('datetime')
            return date.parse('1930-01-01T02:17:43.23-08:00')
        """)
        res = resp[0]
        dt = datetime.datetime.fromisoformat("1930-01-01T02:17:43.230000-08:00")
        assert dt == res

    @ensure_version(min=(2, 10))
    async def test_ext_datetime_write(self, conn: asynctnt.Connection) -> None:
        sp = "tester_ext_datetime"
        dt = datetime.datetime.fromisoformat("2000-01-01T02:17:43.230000-08:00")
        resp = await conn.insert(sp, [1, dt])
        res = resp[0]
        assert dt == res["dt"]

    @ensure_version(min=(2, 10))
    async def test_ext_datetime_write_before_1970(
        self, conn: asynctnt.Connection
    ) -> None:
        sp = "tester_ext_datetime"
        dt = datetime.datetime.fromisoformat("1004-01-01T02:17:43.230000+04:00")
        resp = await conn.insert(sp, [1, dt])
        res = resp[0]
        assert dt == res["dt"]

    @ensure_version(min=(2, 10))
    async def test_ext_datetime_write_without_tz(
        self, conn: asynctnt.Connection
    ) -> None:
        sp = "tester_ext_datetime"
        dt = datetime.datetime.fromisoformat("2022-04-23T02:17:43.450000")
        resp = await conn.insert(sp, [1, dt])
        res = resp[0]
        assert dt == res["dt"]

    @ensure_version(min=(2, 10))
    async def test_ext_datetime_write_without_tz_integer(
        self, conn: asynctnt.Connection
    ) -> None:
        sp = "tester_ext_datetime"
        dt = datetime.datetime.fromisoformat("2022-04-23T02:17:43")
        resp = await conn.insert(sp, [1, dt])
        res = resp[0]
        assert dt == res["dt"]

    @ensure_version(min=(2, 10))
    async def test_ext_datetime_write_pytz(self, conn: asynctnt.Connection) -> None:
        sp = "tester_ext_datetime"
        dt = datetime.datetime.fromisoformat("2022-04-23T02:17:43")
        dt = pytz.timezone("Europe/Amsterdam").localize(dt)
        resp = await conn.insert(sp, [1, dt])
        res = resp[0]
        assert dt == res["dt"]

    @ensure_version(min=(2, 10))
    async def test_ext_datetime_write_pytz_america(
        self, conn: asynctnt.Connection
    ) -> None:
        sp = "tester_ext_datetime"
        dt = datetime.datetime.fromisoformat("2022-04-23T02:17:43")
        dt = pytz.timezone("America/New_York").localize(dt)
        resp = await conn.insert(sp, [1, dt])
        res = resp[0]
        assert dt == res["dt"]


class TestMpExtInterval:
    """Interval extension type tests."""

    @ensure_version(min=(2, 10))
    async def test_ext_interval_read(self, conn: asynctnt.Connection) -> None:
        resp = await conn.eval("""
            local datetime = require('datetime')
            return  datetime.interval.new({
                year=1,
                month=2,
                week=3,
                day=4,
                hour=5,
                min=6,
                sec=7,
                nsec=8,
            })
        """)
        assert resp[0] == asynctnt.MPInterval(
            year=1,
            month=2,
            week=3,
            day=4,
            hour=5,
            min=6,
            sec=7,
            nsec=8,
        )

    @ensure_version(min=(2, 10))
    async def test_ext_interval_read_adjust_last(
        self, conn: asynctnt.Connection
    ) -> None:
        resp = await conn.eval("""
            local datetime = require('datetime')
            return  datetime.interval.new({
                year=1,
                month=2,
                week=3,
                day=4,
                hour=5,
                min=6,
                sec=7,
                nsec=8,
                adjust='last'
            })
        """)
        assert resp[0] == asynctnt.MPInterval(
            year=1,
            month=2,
            week=3,
            day=4,
            hour=5,
            min=6,
            sec=7,
            nsec=8,
            adjust=asynctnt.Adjust.LAST,
        )

    @ensure_version(min=(2, 10))
    async def test_ext_interval_read_adjust_excess(
        self, conn: asynctnt.Connection
    ) -> None:
        resp = await conn.eval("""
            local datetime = require('datetime')
            return  datetime.interval.new({
                year=1,
                month=2,
                week=3,
                day=4,
                hour=5,
                min=6,
                sec=7,
                nsec=8,
                adjust='excess'
            })
        """)
        assert resp[0] == asynctnt.MPInterval(
            year=1,
            month=2,
            week=3,
            day=4,
            hour=5,
            min=6,
            sec=7,
            nsec=8,
            adjust=asynctnt.Adjust.EXCESS,
        )

    @ensure_version(min=(2, 10))
    async def test_ext_interval_read_all_negative(
        self, conn: asynctnt.Connection
    ) -> None:
        resp = await conn.eval("""
            local datetime = require('datetime')
            return  datetime.interval.new({
                year=-1,
                month=-2,
                week=-3,
                day=-4,
                hour=-5,
                min=-6,
                sec=-7,
                nsec=-8,
                adjust='excess'
            })
        """)
        assert resp[0] == asynctnt.MPInterval(
            year=-1,
            month=-2,
            week=-3,
            day=-4,
            hour=-5,
            min=-6,
            sec=-7,
            nsec=-8,
            adjust=asynctnt.Adjust.EXCESS,
        )

    @ensure_version(min=(2, 10))
    async def test_ext_interval_read_all_mixed(self, conn: asynctnt.Connection) -> None:
        resp = await conn.eval("""
            local datetime = require('datetime')
            return  datetime.interval.new({
                year=1,
                month=-2,
                week=3,
                day=-4,
                hour=5,
                min=-6,
                sec=7,
                nsec=-8,
                adjust='excess'
            })
        """)
        assert resp[0] == asynctnt.MPInterval(
            year=1,
            month=-2,
            week=3,
            day=-4,
            hour=5,
            min=-6,
            sec=7,
            nsec=-8,
            adjust=asynctnt.Adjust.EXCESS,
        )

    @ensure_version(min=(2, 10))
    async def test_ext_interval_read_zeros(self, conn: asynctnt.Connection) -> None:
        resp = await conn.eval("""
            local datetime = require('datetime')
            return  datetime.interval.new({})
        """)
        assert resp[0] == asynctnt.MPInterval()

    @ensure_version(min=(2, 10))
    async def test_ext_interval_send(self, conn: asynctnt.Connection) -> None:
        resp = await conn.eval(
            """
            local args = {...}
            local val = args[1]
            local datetime = require('datetime')
            return val == datetime.interval.new({
                year=1,
                month=-2,
                week=3,
                day=-4,
                hour=5,
                min=-6,
                sec=7,
                nsec=-8,
            })
        """,
            [
                asynctnt.MPInterval(
                    year=1,
                    month=-2,
                    week=3,
                    day=-4,
                    hour=5,
                    min=-6,
                    sec=7,
                    nsec=-8,
                )
            ],
        )
        assert resp[0] is True

    @ensure_version(min=(2, 10))
    async def test_ext_interval_send_excess(self, conn: asynctnt.Connection) -> None:
        resp = await conn.eval(
            """
            local args = {...}
            local val = args[1]
            local datetime = require('datetime')
            return val == datetime.interval.new({
                year=1,
                month=-2,
                week=3,
                day=-4,
                hour=5,
                min=-6,
                sec=7,
                nsec=-8,
                adjust='excess'
            })
        """,
            [
                asynctnt.MPInterval(
                    year=1,
                    month=-2,
                    week=3,
                    day=-4,
                    hour=5,
                    min=-6,
                    sec=7,
                    nsec=-8,
                    adjust=asynctnt.Adjust.EXCESS,
                )
            ],
        )
        assert resp[0] is True

    @ensure_version(min=(2, 10))
    async def test_ext_interval_send_with_zeros(
        self, conn: asynctnt.Connection
    ) -> None:
        resp = await conn.eval(
            """
            local args = {...}
            local val = args[1]
            local datetime = require('datetime')
            return val == datetime.interval.new({
                year=100,
            })
        """,
            [
                asynctnt.MPInterval(
                    year=100,
                )
            ],
        )
        assert resp[0] is True
