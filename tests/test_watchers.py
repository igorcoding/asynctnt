from tests import BaseTarantoolTestCase
from tests._testbase import ensure_bin_version, ensure_version


@ensure_bin_version(min=(2, 10))
class WatchersTestCase(BaseTarantoolTestCase):
    @ensure_version(min=(2, 10))
    async def test__basic(self):
        pass
