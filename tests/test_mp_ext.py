import warnings
from decimal import Decimal

from asynctnt import TarantoolTuple
from asynctnt._testbase import ensure_version
from tests import BaseTarantoolTestCase


class MpExtTestCase(BaseTarantoolTestCase):
    TESTER_EXT_SPACE_NAME = 'tester_ext'

    @ensure_version(min=(2, 2))
    async def test__insert_decimal(self):
        dec = Decimal('-12.34')
        res = await self.conn.replace(self.TESTER_EXT_SPACE_NAME, [1, dec])
        self.assertEqual(res[0][1], dec)

        dec = Decimal('-12.4')
        res = await self.conn.replace(self.TESTER_EXT_SPACE_NAME, [1, dec])
        self.assertEqual(res[0][1], dec)

        dec = Decimal('0')
        res = await self.conn.replace(self.TESTER_EXT_SPACE_NAME, [1, dec])
        self.assertEqual(res[0][1], dec)

        dec = Decimal(42)
        res = await self.conn.replace(self.TESTER_EXT_SPACE_NAME, [1, dec])
        self.assertEqual(res[0][1], dec)

        dec = Decimal('0.33')
        res = await self.conn.replace(self.TESTER_EXT_SPACE_NAME, [1, dec])
        self.assertEqual(res[0][1], dec)

        dec = Decimal('0.000000000000000000000000000000000010')
        res = await self.conn.replace(self.TESTER_EXT_SPACE_NAME, [1, dec])
        self.assertEqual(res[0][1], dec)

        dec = Decimal('-0.000000000000000000000000000000000010')
        res = await self.conn.replace(self.TESTER_EXT_SPACE_NAME, [1, dec])
        self.assertEqual(res[0][1], dec)
