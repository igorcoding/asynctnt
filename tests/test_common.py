import asyncio

from asynctnt import Iterator
from asynctnt import Response
from asynctnt.exceptions import TarantoolSchemaError
from tests import BaseTarantoolTestCase
from tests.util import get_complex_param


class CommonTestCase(BaseTarantoolTestCase):
    async def test__encoding_utf8(self):
        p, p_cmp = get_complex_param(replace_bin=False)

        data = [1, 'hello', p]
        data_cmp = [1, 'hello', p_cmp]

        res = await self.conn.insert(self.TESTER_SPACE_ID, data)
        self.assertListEqual(res.body, [data_cmp], 'Body ok')

        res = await self.conn.select(self.TESTER_SPACE_ID)
        self.assertListEqual(res.body, [data_cmp], 'Body ok')

    async def test__encoding_cp1251(self):
        await self.tnt_reconnect(encoding='cp1251')
        p, p_cmp = get_complex_param(replace_bin=False)

        data = [1, 'hello', p]
        data_cmp = [1, 'hello', p_cmp]

        res = await self.conn.insert(self.TESTER_SPACE_ID, data)
        self.assertListEqual(res.body, [data_cmp], 'Body ok')

        res = await self.conn.select(self.TESTER_SPACE_ID)
        self.assertListEqual(res.body, [data_cmp], 'Body ok')

