import asyncio
import unittest
import os

import asynctnt
from asynctnt.exceptions import SSLError
from asynctnt.instance import TarantoolSyncInstance
from tests import BaseTarantoolTestCase

def is_test_ssl():
    env = os.getenv("TEST_TT_SSL")
    if env:
        env = env.upper()
        return env == "1" or env == "TRUE"
    return False


@unittest.skipIf(not is_test_ssl(), "TEST_TT_SSL is not set.")
class SSLTestCase(BaseTarantoolTestCase):
    DO_CONNECT = False

    ssl_files_dir = os.path.join(os.getcwd(), 'tests', 'files', 'ssl')
    cert_file = os.path.join(ssl_files_dir, "localhost.crt")
    invalidhost_cert_file = os.path.join(ssl_files_dir, "invalidhost.crt")
    key_file = os.path.join(ssl_files_dir, "localhost.key")
    ca_file = os.path.join(ssl_files_dir, "ca.crt")
    empty_file = os.path.join(ssl_files_dir, "empty")
    invalid_file = "any_invalid_path"

    async def test__connect(self):
        if self.in_docker:
            self.skipTest('Skipping as running inside the docker')
            return

        class SslTestSubcase:
            def __init__(self,
                         name="",
                         expectSSLError=False,
                         expectTimeoutError=False,
                         server_transport=asynctnt.Transport.SSL,
                         server_key_file=None,
                         server_cert_file=None,
                         server_ca_file=None,
                         server_ciphers=None,
                         client_transport=asynctnt.Transport.SSL,
                         client_cert_file=None,
                         client_key_file=None,
                         client_ca_file=None,
                         client_ciphers=None):
                self.name = name
                self.expectSSLError = expectSSLError
                self.expectTimeoutError = expectTimeoutError
                self.server_transport = server_transport
                self.server_key_file = server_key_file
                self.server_cert_file = server_cert_file
                self.server_ca_file = server_ca_file
                self.server_ciphers = server_ciphers
                self.client_transport = client_transport
                self.client_cert_file = client_cert_file
                self.client_key_file = client_key_file
                self.client_ca_file = client_ca_file
                self.client_ciphers = client_ciphers

        # Requirements from Tarantool Enterprise Edition manual:
        # https://www.tarantool.io/en/enterprise_doc/security/#configuration
        #
        # For a server:
        # ssl_key_file - mandatory
        # ssl_cert_file - mandatory
        # ssl_ca_file - optional
        # ssl_ciphers - optional
        #
        # For a client:
        # ssl_key_file - optional, mandatory if server.CaFile set
        # ssl_cert_file - optional, mandatory if server.CaFile set
        # ssl_ca_file - optional
        # ssl_ciphers - optional
        testcases = [
            SslTestSubcase(
                name="no_ssl_server",
                expectSSLError=True,
                server_transport=asynctnt.Transport.DEFAULT),
            SslTestSubcase(
                name="key_crt_server",
                server_key_file=self.key_file,
                server_cert_file=self.cert_file),
            SslTestSubcase(
                name="no_ssl_client",
                expectTimeoutError=True,
                server_key_file=self.key_file,
                server_cert_file=self.cert_file,
                client_transport=asynctnt.Transport.DEFAULT),
            SslTestSubcase(
                name="key_crt_server_and_client",
                server_key_file=self.key_file,
                server_cert_file=self.cert_file,
                client_key_file=self.key_file,
                client_cert_file=self.cert_file),
            SslTestSubcase(
                name="key_crt_ca_server",
                expectSSLError=True,
                server_key_file=self.key_file,
                server_cert_file=self.cert_file,
                server_ca_file=self.ca_file),
            SslTestSubcase(
                name="key_crt_ca_server_and_crt_client",
                expectSSLError=True,
                server_key_file=self.key_file,
                server_cert_file=self.cert_file,
                server_ca_file=self.ca_file,
                client_cert_file=self.cert_file),
            SslTestSubcase(
                name="key_crt_ca_server_and_key_crt_client",
                server_key_file=self.key_file,
                server_cert_file=self.cert_file,
                server_ca_file=self.ca_file,
                client_key_file=self.key_file,
                client_cert_file=self.cert_file),
            SslTestSubcase(
                name="key_crt_ca_server_and_client",
                server_key_file=self.key_file,
                server_cert_file=self.cert_file,
                server_ca_file=self.ca_file,
                client_key_file=self.key_file,
                client_cert_file=self.cert_file,
                client_ca_file=self.ca_file),
            SslTestSubcase(
                name="key_invalidhost_crt_ca_server_and_key_crt_ca_client",
                # A Tarantool implementation does not check hostname. It's
                # the expected behavior. We don't do that too.
                server_key_file=self.key_file,
                server_cert_file=self.invalidhost_cert_file,
                server_ca_file=self.ca_file,
                client_key_file=self.key_file,
                client_cert_file=self.cert_file,
                client_ca_file=self.ca_file),
            SslTestSubcase(
                name="key_crt_ca_server_and_client_invalid_crt",
                expectSSLError=True,
                client_key_file=self.key_file,
                server_cert_file=self.cert_file,
                server_ca_file=self.ca_file,
                server_key_file=self.key_file,
                client_cert_file=self.invalid_file,
                client_ca_file=self.ca_file),
            SslTestSubcase(
                name="key_crt_ca_server_and_client_invalid_key",
                expectSSLError=True,
                server_key_file=self.key_file,
                server_cert_file=self.cert_file,
                server_ca_file=self.ca_file,
                client_key_file=self.invalid_file,
                client_cert_file=self.cert_file,
                client_ca_file=self.ca_file),
            SslTestSubcase(
                name="key_crt_ca_server_and_client_invalid_ca",
                expectSSLError=True,
                server_key_file=self.key_file,
                server_cert_file=self.cert_file,
                server_ca_file=self.ca_file,
                client_key_file=self.key_file,
                client_cert_file=self.cert_file,
                client_ca_file=self.invalid_file),
            SslTestSubcase(
                name="key_crt_ca_server_and_client_empty_crt",
                expectSSLError=True,
                server_key_file=self.key_file,
                server_cert_file=self.cert_file,
                server_ca_file=self.ca_file,
                client_key_file=self.key_file,
                client_cert_file=self.empty_file,
                client_ca_file=self.ca_file),
            SslTestSubcase(
                name="key_crt_ca_server_and_client_empty_key",
                expectSSLError=True,
                server_key_file=self.key_file,
                server_cert_file=self.cert_file,
                server_ca_file=self.ca_file,
                client_key_file=self.empty_file,
                client_cert_file=self.cert_file,
                client_ca_file=self.ca_file),
            SslTestSubcase(
                name="key_crt_ca_server_and_client_empty_ca",
                expectSSLError=True,
                server_key_file=self.key_file,
                server_cert_file=self.cert_file,
                server_ca_file=self.ca_file,
                client_key_file=self.key_file,
                client_cert_file=self.cert_file,
                client_ca_file=self.empty_file),
            SslTestSubcase(
                name="key_crt_ca_ciphers_server_and_key_crt_ca_client",
                server_key_file=self.key_file,
                server_cert_file=self.cert_file,
                server_ca_file=self.ca_file,
                server_ciphers="ECDHE-RSA-AES256-GCM-SHA384",
                client_key_file=self.key_file,
                client_cert_file=self.cert_file,
                client_ca_file=self.ca_file),
            SslTestSubcase(
                name="key_crt_ca_ciphers_server_and_client",
                server_key_file=self.key_file,
                server_cert_file=self.cert_file,
                server_ca_file=self.ca_file,
                server_ciphers="ECDHE-RSA-AES256-GCM-SHA384",
                client_key_file=self.key_file,
                client_cert_file=self.cert_file,
                client_ca_file=self.ca_file,
                client_ciphers="ECDHE-RSA-AES256-GCM-SHA384"),
            SslTestSubcase(
                name="non_equal_ciphers",
                expectSSLError=True,
                server_key_file=self.key_file,
                server_cert_file=self.cert_file,
                server_ca_file=self.ca_file,
                server_ciphers="ECDHE-RSA-AES256-GCM-SHA384",
                client_key_file=self.key_file,
                client_cert_file=self.cert_file,
                client_ca_file=self.ca_file,
                client_ciphers="TLS_AES_128_GCM_SHA256"),
            SslTestSubcase(
                name="key_crt_ca_multiple_ciphers_server_and_client",
                server_key_file=self.key_file,
                server_cert_file=self.cert_file,
                server_ca_file=self.ca_file,
                server_ciphers="ECDHE-RSA-AES256-GCM-SHA384:TLS_AES_128_GCM_SHA256",
                client_key_file=self.key_file,
                client_cert_file=self.cert_file,
                client_ca_file=self.ca_file,
                client_ciphers="ECDHE-RSA-AES256-GCM-SHA384:TLS_AES_128_GCM_SHA256"),
        ]

        for t in testcases:
            with self.subTest(msg=t.name):
                tnt = TarantoolSyncInstance(
                    port=TarantoolSyncInstance.get_random_port(),
                    transport=t.server_transport,
                    ssl_key_file=t.server_key_file,
                    ssl_cert_file=t.server_cert_file,
                    ssl_ca_file=t.server_ca_file,
                    ssl_ciphers=t.server_ciphers,
                    applua=self.read_applua(),
                    cleanup=self.TNT_CLEANUP,
                )

                tnt.start()
                try:
                    conn = await asynctnt.connect(host=tnt.host, port=tnt.port,
                                                  transport=t.client_transport,
                                                  ssl_key_file=t.client_key_file,
                                                  ssl_cert_file=t.client_cert_file,
                                                  ssl_ca_file=t.client_ca_file,
                                                  ssl_ciphers=t.client_ciphers,
                                                  reconnect_timeout=0)

                    tupl = [1, 'hello', 1, 4, 'what is up']
                    await conn.insert(self.TESTER_SPACE_ID, tupl)
                    res = await conn.select(self.TESTER_SPACE_NAME, tupl[0:1])
                    self.assertResponseEqual(res[0], tupl, 'Tuple ok')
                except SSLError as e:
                    if not t.expectSSLError:
                        self.fail(e)
                except asyncio.TimeoutError as e:
                    if not t.expectTimeoutError:
                        self.fail(e)
                except Exception as e:
                    self.fail(e)
                finally:
                    tnt.stop()        
