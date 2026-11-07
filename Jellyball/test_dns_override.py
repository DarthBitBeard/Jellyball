"""Optional per-app DNS (DNS_SERVERS / DNS_OVER_HTTPS): internet names resolve
through the chosen resolvers, local names through the system, and a resolver
outage falls back to the system."""

import asyncio
import socket
import unittest
from unittest.mock import patch

import dns_override


class DnsOverrideTests(unittest.TestCase):
    def setUp(self):
        self.queries = []

        def fake_query(name, rdtype):
            self.queries.append((name, rdtype))
            if name == "cdn.example.test" and rdtype == "A":
                return ["203.0.113.7"], 120.0
            return [], 300.0

        self._patch = patch.object(dns_override, "_query", side_effect=fake_query)
        self._patch.start()
        self.system = []
        original = dns_override._original_getaddrinfo

        def system_getaddrinfo(host, *args, **kwargs):
            self.system.append(host)
            return original(host, *args, **kwargs) if host in ("127.0.0.1", "203.0.113.7") else [
                (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("0.0.0.0", 443))  # what a sinkhole answers
            ]

        self._sys = patch.object(dns_override, "_original_getaddrinfo", side_effect=system_getaddrinfo)
        self._sys.start()

    def tearDown(self):
        self._sys.stop()
        self._patch.stop()
        dns_override.uninstall()

    def test_off_by_default(self):
        with patch.dict("os.environ", {"DNS_SERVERS": "", "DNS_OVER_HTTPS": ""}):
            self.assertFalse(dns_override.install_from_env())
        self.assertEqual(dns_override.describe(), "system resolver")

    def test_internet_names_use_the_chosen_resolver(self):
        with patch.dict("os.environ", {"DNS_SERVERS": "8.8.8.8, 9.9.9.9", "DNS_OVER_HTTPS": ""}):
            self.assertTrue(dns_override.install_from_env())
        infos = socket.getaddrinfo("cdn.example.test", 443, type=socket.SOCK_STREAM)
        self.assertEqual({info[4][0] for info in infos}, {"203.0.113.7"})
        self.assertNotIn("cdn.example.test", self.system)  # never asked the (sinkholing) system resolver
        self.assertEqual(dns_override.describe(), "DNS servers 8.8.8.8, 9.9.9.9")

    def test_answers_are_cached(self):
        dns_override.configure("8.8.8.8")
        dns_override.resolve("cdn.example.test")
        dns_override.resolve("cdn.example.test")
        self.assertEqual(self.queries.count(("cdn.example.test", "A")), 1)

    def test_local_names_use_the_system_resolver(self):
        dns_override.configure("8.8.8.8")
        for host in ("localhost", "jellyfin", "nas.local", "router.lan", "box.home.arpa", "127.0.0.1"):
            with self.subTest(host=host):
                self.assertTrue(dns_override._is_local_name(host))
        self.assertFalse(dns_override._is_local_name("cdn.example.test"))

    def test_resolver_outage_falls_back_to_the_system(self):
        with patch.dict("os.environ", {"DNS_SERVERS": "8.8.8.8", "DNS_OVER_HTTPS": ""}):
            dns_override.install_from_env()
        infos = socket.getaddrinfo("unknown.example.test", 443)
        self.assertIn("unknown.example.test", self.system)
        self.assertEqual(infos[0][4][0], "0.0.0.0")

    def test_doh_wins_and_its_own_host_resolves_normally(self):
        dns_override.configure("8.8.8.8", "https://dns.example.test/dns-query")
        self.assertEqual(dns_override.describe(), "DNS over HTTPS (dns.example.test)")
        self.assertTrue(dns_override._is_local_name("dns.example.test"))  # no recursion into itself

    def test_invalid_settings_are_ignored(self):
        self.assertFalse(dns_override.configure("not-an-ip, dns.google", "http://insecure.example.test/q"))

    def test_asyncio_lookups_go_through_the_override(self):
        dns_override.configure("8.8.8.8")
        dns_override._installed = False
        with patch.dict("os.environ", {"DNS_SERVERS": "8.8.8.8", "DNS_OVER_HTTPS": ""}):
            dns_override.install_from_env()

        async def lookup():
            return await asyncio.get_running_loop().getaddrinfo("cdn.example.test", 443, type=socket.SOCK_STREAM)

        infos = asyncio.run(lookup())
        self.assertEqual({info[4][0] for info in infos}, {"203.0.113.7"})


if __name__ == "__main__":
    unittest.main()
