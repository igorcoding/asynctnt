import uuid
from decimal import Decimal

from asynctnt._testbase import ensure_version
from tests import BaseTarantoolTestCase


class MpExtTestCase(BaseTarantoolTestCase):

    @ensure_version(min=(2, 2))
    async def test__decimal(self):
        space = 'tester_ext_dec'

        dec = Decimal('-12.34')
        res = await self.conn.replace(space, [1, dec])
        self.assertEqual(res[0][1], dec)

        dec = Decimal('-12.345')
        res = await self.conn.replace(space, [1, dec])
        self.assertEqual(res[0][1], dec)

        dec = Decimal('-12.4')
        res = await self.conn.replace(space, [1, dec])
        self.assertEqual(res[0][1], dec)

        dec = Decimal('0')
        res = await self.conn.replace(space, [1, dec])
        self.assertEqual(res[0][1], dec)

        dec = Decimal('0.000')
        res = await self.conn.replace(space, [1, dec])
        self.assertEqual(res[0][1], dec)

        dec = Decimal(42)
        res = await self.conn.replace(space, [1, dec])
        self.assertEqual(res[0][1], dec)

        dec = Decimal('0.33')
        res = await self.conn.replace(space, [1, dec])
        self.assertEqual(res[0][1], dec)

        dec = Decimal('0.000000000000000000000000000000000010')
        res = await self.conn.replace(space, [1, dec])
        self.assertEqual(res[0][1], dec)

        dec = Decimal('-0.000000000000000000000000000000000010')
        res = await self.conn.replace(space, [1, dec])
        self.assertEqual(res[0][1], dec)

        dec = Decimal('0.1111111111111111')
        res = await self.conn.replace(space, [1, dec])
        self.assertEqual(res[0][1], dec)

        dec = Decimal('-0.1111111111111111')
        res = await self.conn.replace(space, [1, dec])
        self.assertEqual(res[0][1], dec)

        dec = Decimal('-0.111111')
        res = await self.conn.replace(space, [1, dec])
        self.assertEqual(res[0][1], dec)

    @ensure_version(min=(2, 4, 1))
    async def test__uuid(self):
        space = 'tester_ext_uuid'

        val = uuid.uuid4()
        res = await self.conn.replace(space, [1, val])
        self.assertEqual(res[0][1], val)

        val = uuid.UUID('f6423bdf-b49e-4913-b361-0740c9702e4b')
        res = await self.conn.replace(space, [1, val])
        self.assertEqual(res[0][1], val)

        val = uuid.UUID('00000000-0000-0000-0000-000000000000')
        res = await self.conn.replace(space, [1, val])
        self.assertEqual(res[0][1], val)

        val = uuid.uuid1(1, 100)
        res = await self.conn.replace(space, [1, val])
        self.assertEqual(res[0][1], val)

        val = uuid.uuid3(uuid.uuid4(), "hellothere")
        res = await self.conn.replace(space, [1, val])
        self.assertEqual(res[0][1], val)

        val = uuid.uuid5(uuid.NAMESPACE_URL, "generalkenobi")
        res = await self.conn.replace(space, [1, val])
        self.assertEqual(res[0][1], val)
