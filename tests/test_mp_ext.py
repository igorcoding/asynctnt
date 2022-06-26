import uuid
from decimal import Decimal

from asynctnt import IProtoError
from asynctnt._testbase import ensure_version
from asynctnt.exceptions import TarantoolDatabaseError, ErrorCode
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

    @ensure_version(min=(2, 4, 1))
    async def test__ext_error(self):
        try:
            await self.conn.eval("""
                box.schema.space.create('_space')
            """)
        except TarantoolDatabaseError as e:
            self.assertIsNotNone(e.error)
            self.assertGreater(len(e.error.trace), 0)
            frame = e.error.trace[0]
            self.assertEqual('ClientError', frame.error_type)
            self.assertIsNotNone(frame.file)
            self.assertIsNotNone(frame.line)
            self.assertEqual("Space '_space' already exists", frame.message)
            self.assertEqual(0, frame.err_no)
            self.assertEqual(ErrorCode.ER_SPACE_EXISTS, frame.code)

    @ensure_version(min=(2, 4, 1))
    async def test__ext_error_custom(self):
        try:
            await self.conn.eval("""
                local e = box.error.new{code=5,reason='A',type='B'}
                box.error(e)
            """)
        except TarantoolDatabaseError as e:
            self.assertIsNotNone(e.error)
            self.assertGreater(len(e.error.trace), 0)
            frame = e.error.trace[0]
            self.assertEqual('CustomError', frame.error_type)
            self.assertIsNotNone(frame.file)
            self.assertIsNotNone(frame.line)
            self.assertEqual("A", frame.message)
            self.assertEqual(0, frame.err_no)
            self.assertEqual(5, frame.code)
            self.assertIn('custom_type', frame.fields)
            self.assertEqual('B', frame.fields['custom_type'])

    @ensure_version(min=(2, 4, 1))
    async def test__ext_error_custom_return(self):
        resp = await self.conn.eval("""
            local e = box.error.new{code=5,reason='A',type='B'}
            return e
        """)
        e = resp.body[0]
        self.assertIsInstance(e, IProtoError)
        self.assertGreater(len(e.trace), 0)
        frame = e.trace[0]
        self.assertEqual('CustomError', frame.error_type)
        self.assertEqual('eval', frame.file)
        self.assertEqual(2, frame.line)
        self.assertEqual("A", frame.message)
        self.assertEqual(0, frame.err_no)
        self.assertEqual(5, frame.code)
        self.assertIn('custom_type', frame.fields)
        self.assertEqual('B', frame.fields['custom_type'])
