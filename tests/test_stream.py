import asyncio

from asynctnt._testbase import ensure_version, ensure_bin_version
from asynctnt.exceptions import TarantoolDatabaseError, ErrorCode
from tests import BaseTarantoolTestCase


@ensure_bin_version(min=(2, 10))
class StreamTestCase(BaseTarantoolTestCase):
    EXTRA_BOX_CFG = "memtx_use_mvcc_engine = true"

    @ensure_version(min=(2, 10))
    async def test__transaction_commit(self):
        s = self.conn.stream()
        self.assertGreater(s.stream_id, 0)

        await s.begin()
        data = [1, 'hello', 1, 4, 'what is up']
        await s.insert(self.TESTER_SPACE_NAME, data)
        res = await s.select(self.TESTER_SPACE_NAME)
        self.assertResponseEqual(res, [data])

        await s.commit()

        res = await self.conn.select(self.TESTER_SPACE_NAME)
        self.assertResponseEqual(res, [data])

    @ensure_version(min=(2, 10))
    async def test__transaction_rolled_back(self):
        s = self.conn.stream()
        await s.begin()
        data = [1, 'hello', 1, 4, 'what is up']
        await s.insert(self.TESTER_SPACE_NAME, data)
        res = await s.select(self.TESTER_SPACE_NAME)
        self.assertResponseEqual(res, [data])

        await s.rollback()

        res = await self.conn.select(self.TESTER_SPACE_NAME)
        self.assertResponseEqual(res, [])

    @ensure_version(min=(2, 10))
    async def test__transaction_begin_through_call(self):
        s = self.conn.stream()
        await s.call('box.begin')
        data = [1, 'hello', 1, 4, 'what is up']
        await s.insert(self.TESTER_SPACE_NAME, data)
        res = await s.select(self.TESTER_SPACE_NAME)
        self.assertResponseEqual(res, [data])

        await s.commit()

        res = await self.conn.select(self.TESTER_SPACE_NAME)
        self.assertResponseEqual(res, [data])

    @ensure_version(min=(2, 10))
    async def test__transaction_commit_through_call(self):
        s = self.conn.stream()
        await s.begin()
        data = [1, 'hello', 1, 4, 'what is up']
        await s.insert(self.TESTER_SPACE_NAME, data)
        res = await s.select(self.TESTER_SPACE_NAME)
        self.assertResponseEqual(res, [data])

        await s.call('box.commit')

        res = await self.conn.select(self.TESTER_SPACE_NAME)
        self.assertResponseEqual(res, [data])

    @ensure_version(min=(2, 10))
    async def test__transaction_rolled_back_through_call(self):
        s = self.conn.stream()
        await s.begin()
        data = [1, 'hello', 1, 4, 'what is up']
        await s.insert(self.TESTER_SPACE_NAME, data)
        res = await s.select(self.TESTER_SPACE_NAME)
        self.assertResponseEqual(res, [data])

        await s.call('box.rollback')

        res = await self.conn.select(self.TESTER_SPACE_NAME)
        self.assertResponseEqual(res, [])

    @ensure_version(min=(2, 10))
    async def test__transaction_commit_through_sql(self):
        s = self.conn.stream()
        await s.execute('START TRANSACTION')
        data = [1, 'hello', 1, 4, 'what is up']
        await s.insert(self.TESTER_SPACE_NAME, data)
        res = await s.select(self.TESTER_SPACE_NAME)
        self.assertResponseEqual(res, [data])

        await s.execute('COMMIT')

        res = await self.conn.select(self.TESTER_SPACE_NAME)
        self.assertResponseEqual(res, [data])

    @ensure_version(min=(2, 10))
    async def test__transaction_rollback_through_sql(self):
        s = self.conn.stream()
        await s.execute('START TRANSACTION')
        data = [1, 'hello', 1, 4, 'what is up']
        await s.insert(self.TESTER_SPACE_NAME, data)
        res = await s.select(self.TESTER_SPACE_NAME)
        self.assertResponseEqual(res, [data])

        await s.execute('ROLLBACK')

        res = await self.conn.select(self.TESTER_SPACE_NAME)
        self.assertResponseEqual(res, [])

    @ensure_version(min=(2, 10))
    async def test__transaction_context_manager_commit(self):
        data = [1, 'hello', 1, 4, 'what is up']

        async with self.conn.stream() as s:
            await s.insert(self.TESTER_SPACE_NAME, data)
            res = await s.select(self.TESTER_SPACE_NAME)
            self.assertResponseEqual(res, [data])

        res = await self.conn.select(self.TESTER_SPACE_NAME)
        self.assertResponseEqual(res, [data])

    @ensure_version(min=(2, 10))
    async def test__transaction_context_manager_rollback(self):
        class ExpectedError(Exception):
            pass

        data = [1, 'hello', 1, 4, 'what is up']

        try:
            async with self.conn.stream() as s:
                await s.insert(self.TESTER_SPACE_NAME, data)
                res = await s.select(self.TESTER_SPACE_NAME)
                self.assertResponseEqual(res, [data])

                raise ExpectedError("some error")
        except ExpectedError:
            pass

        res = await self.conn.select(self.TESTER_SPACE_NAME)
        self.assertResponseEqual(res, [])

    @ensure_version(min=(2, 10))
    async def test__transaction_2_streams(self):
        data1 = [1, 'hello', 1, 4, 'what is up']
        data2 = [2, 'hi', 100, 400, 'nothing match']

        s1 = self.conn.stream()
        s2 = self.conn.stream()

        await s1.begin()
        await s2.begin()

        await s1.insert(self.TESTER_SPACE_NAME, data1)
        await s2.insert(self.TESTER_SPACE_NAME, data2)

        res = await s1.select(self.TESTER_SPACE_NAME, [1])
        self.assertResponseEqual(res, [data1])

        res = await s2.select(self.TESTER_SPACE_NAME, [2])
        self.assertResponseEqual(res, [data2])

        await s1.commit()
        await s2.commit()

        res = await self.conn.select(self.TESTER_SPACE_NAME)
        self.assertResponseEqual(res, [data1, data2])

    @ensure_version(min=(2, 10))
    async def test__transaction_timeout(self):
        s = self.conn.stream()
        await s.begin(tx_timeout=0.5)

        await asyncio.sleep(1.0)

        with self.assertRaises(TarantoolDatabaseError) as exc:
            await s.commit()

        self.assertEqual(ErrorCode.ER_TRANSACTION_TIMEOUT, exc.exception.code)
        self.assertEqual('Transaction has been aborted by timeout',
                         exc.exception.message)
