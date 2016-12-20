from asynctnt._testbase import TestCase
from asynctnt.demo import super_func


class DemoTestCase(TestCase):
    async def test_super_func(self):
        a = 5
        b = 6
        res = await super_func(a, b, loop=self.loop)
        self.assertEqual(res, a + b)
