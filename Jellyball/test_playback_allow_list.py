"""X4: PLAYBACK_ALLOW_CIDRS and TRUSTED_PROXIES.

The allow-list must be a no-op while unset (the 2.0 behaviour), must always
let loopback through (Multi-View's ffmpeg and the relay fetch our own
streams), and the client address must not be spoofable with
X-Forwarded-For unless the peer is a trusted proxy.
"""

import asyncio
import ipaddress
import unittest
from unittest.mock import patch

import httpx

import epg
import http_hardening
import main
import security
import state
from http_hardening import client_ip, is_playback_path, parse_ip, parse_networks, playback_allowed
from state import new_channel_state


def _nets(text: str):
    return parse_networks(text, "TEST")[0]


def _scope(peer: str, xff: str = "") -> dict:
    headers = [(b"x-forwarded-for", xff.encode())] if xff else []
    return {"type": "http", "client": (peer, 50000), "headers": headers}


async def _no_tvguide():
    return {}


def _get(path: str, *, peer: str, allow: str = None, trusted: str = "", headers=None, auth=None) -> httpx.Response:
    allow_list = None if allow is None else _nets(allow)

    async def go():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=main.app, client=(peer, 50000)),
            base_url="http://127.0.0.1:8000", follow_redirects=False,
        ) as client:
            return await client.get(path, headers=headers, auth=auth)

    with patch.object(http_hardening, "PLAYBACK_ALLOW_CIDRS", allow_list), \
            patch.object(http_hardening, "TRUSTED_PROXIES", _nets(trusted)), \
            patch.object(security, "DASHBOARD_PASSWORD", "secret"), \
            patch.object(security, "DASHBOARD_USERNAME", "admin"), \
            patch.dict(security._AUTH_FAILURES, clear=True), \
            patch.object(epg, "_fetch_tvguide_epg", _no_tvguide):
        return asyncio.run(go())


class ClientAddressTests(unittest.TestCase):
    def test_without_trusted_proxies_x_forwarded_for_is_ignored(self):
        self.assertEqual(str(client_ip(_scope("203.0.113.7", "192.168.1.10"), [])), "203.0.113.7")

    def test_an_untrusted_peer_cannot_spoof_its_address(self):
        proxies = _nets("10.0.0.2")
        self.assertEqual(str(client_ip(_scope("203.0.113.7", "192.168.1.10"), proxies)), "203.0.113.7")

    def test_a_trusted_proxy_supplies_the_client_address(self):
        proxies = _nets("10.0.0.2")
        self.assertEqual(str(client_ip(_scope("10.0.0.2", "192.168.1.10"), proxies)), "192.168.1.10")

    def test_spoofed_entries_left_of_the_real_client_are_ignored(self):
        # The client sent "X-Forwarded-For: 192.168.1.10"; the proxy appended the real address.
        proxies = _nets("10.0.0.2")
        scope = _scope("10.0.0.2", "192.168.1.10, 203.0.113.7")
        self.assertEqual(str(client_ip(scope, proxies)), "203.0.113.7")

    def test_a_chain_of_trusted_proxies_is_walked_right_to_left(self):
        proxies = _nets("10.0.0.0/24")
        scope = _scope("10.0.0.2", "198.51.100.4, 10.0.0.3")
        self.assertEqual(str(client_ip(scope, proxies)), "198.51.100.4")

    def test_multiple_header_lines_are_combined(self):
        proxies = _nets("10.0.0.2")
        scope = {"type": "http", "client": ("10.0.0.2", 1), "headers": [
            (b"x-forwarded-for", b"192.168.1.10"), (b"X-Forwarded-For", b"203.0.113.7"),
        ]}
        self.assertEqual(str(client_ip(scope, proxies)), "203.0.113.7")

    def test_garbage_from_a_trusted_proxy_fails_closed(self):
        proxies = _nets("10.0.0.2")
        self.assertIsNone(client_ip(_scope("10.0.0.2", "not-an-ip, 203.0.113.7x"), proxies))

    def test_no_peer_is_unknown(self):
        self.assertIsNone(client_ip({"type": "http", "headers": []}, []))

    def test_address_forms(self):
        self.assertEqual(str(parse_ip("[::1]")), "::1")
        self.assertEqual(str(parse_ip("192.0.2.1:8080")), "192.0.2.1")
        self.assertEqual(str(parse_ip("::ffff:192.0.2.1")), "192.0.2.1")
        self.assertEqual(str(parse_ip("fe80::1%eth0")), "fe80::1")
        self.assertIsNone(parse_ip("unknown"))
        self.assertIsNone(parse_ip(""))

    def test_invalid_network_entries_are_reported_not_used(self):
        with self.assertLogs("jellyball", level="ERROR"):
            networks, invalid = parse_networks("192.168.1.0/24, banana 10.0.0.1", "PLAYBACK_ALLOW_CIDRS")
        self.assertEqual([str(n) for n in networks], ["192.168.1.0/24", "10.0.0.1/32"])
        self.assertEqual(invalid, ["banana"])

    def test_uvicorn_keeps_its_default_unless_proxies_are_configured(self):
        with patch.object(http_hardening, "TRUSTED_PROXIES", []):
            self.assertIsNone(http_hardening.uvicorn_forwarded_allow_ips())
        with patch.object(http_hardening, "TRUSTED_PROXIES", _nets("10.0.0.2, 172.16.0.0/12")):
            self.assertEqual(http_hardening.uvicorn_forwarded_allow_ips(), ["10.0.0.2/32", "172.16.0.0/12"])


class AllowListRuleTests(unittest.TestCase):
    def test_unset_allows_everyone(self):
        self.assertTrue(playback_allowed(ipaddress.ip_address("203.0.113.7"), None))
        self.assertTrue(playback_allowed(None, None))

    def test_listed_networks_and_loopback_only(self):
        allow = _nets("192.168.1.0/24")
        self.assertTrue(playback_allowed(ipaddress.ip_address("192.168.1.50"), allow))
        self.assertTrue(playback_allowed(ipaddress.ip_address("127.0.0.1"), allow))
        self.assertTrue(playback_allowed(ipaddress.ip_address("::1"), allow))
        self.assertFalse(playback_allowed(ipaddress.ip_address("192.168.2.50"), allow))
        self.assertFalse(playback_allowed(None, allow))

    def test_a_setting_with_only_invalid_entries_fails_closed(self):
        with patch.dict("os.environ", {"PLAYBACK_ALLOW_CIDRS": "banana"}), self.assertLogs("jellyball", "ERROR"):
            allow = http_hardening._load_playback_allow_list()
        self.assertEqual(allow, [])
        self.assertFalse(playback_allowed(ipaddress.ip_address("192.168.1.50"), allow))
        self.assertTrue(playback_allowed(ipaddress.ip_address("127.0.0.1"), allow))
        with patch.dict("os.environ", {"PLAYBACK_ALLOW_CIDRS": "  "}):
            self.assertIsNone(http_hardening._load_playback_allow_list())

    def test_which_paths_are_playback(self):
        for path in ("/playlist.m3u", "/epg.xml", "/stream/nfl_buf.m3u8", "/stream/nfl_buf/seg/3.ts",
                     "/stream/nfl_buf", "/multiview/mv/audio-0.m3u8", "/multiview/mv/audio/0/seg/1.ts",
                     "/chunk", "/chunk.ts", "/chunk.mp4", "/chunk.aac", "/chunk.vtt", "/resource",
                     "/substream.m3u8"):
            with self.subTest(path=path):
                self.assertTrue(is_playback_path(path))
        for path in ("/", "/healthz", "/api/status", "/metrics", "/multiview/create", "/multiview/mv/stop",
                     "/playlist.m3u.bak", "/chunkx", "/login", "/static/dashboard.js"):
            with self.subTest(path=path):
                self.assertFalse(is_playback_path(path))


class PlaybackMiddlewareTests(unittest.TestCase):
    def setUp(self):
        self._backup = dict(state.stream_state)
        state.stream_state.clear()
        state.stream_state["nfl_buf"] = new_channel_state(name="Buffalo Bills", query="Buffalo Bills", category="nfl")

    def tearDown(self):
        state.stream_state.clear()
        state.stream_state.update(self._backup)

    def test_unset_serves_everyone_as_before(self):
        self.assertEqual(_get("/playlist.m3u", peer="203.0.113.7").status_code, 200)

    def test_outside_the_list_is_refused(self):
        for path in ("/playlist.m3u", "/epg.xml", "/stream/nfl_buf.m3u8", "/chunk.ts"):
            with self.subTest(path=path):
                response = _get(path, peer="203.0.113.7", allow="192.168.1.0/24")
                self.assertEqual(response.status_code, 403)

    def test_inside_the_list_and_loopback_are_served(self):
        self.assertEqual(_get("/playlist.m3u", peer="192.168.1.20", allow="192.168.1.0/24").status_code, 200)
        self.assertEqual(_get("/playlist.m3u", peer="127.0.0.1", allow="192.168.1.0/24").status_code, 200)

    def test_spoofed_forwarded_for_does_not_get_in(self):
        response = _get("/playlist.m3u", peer="203.0.113.7", allow="192.168.1.0/24",
                        headers={"X-Forwarded-For": "192.168.1.20"})
        self.assertEqual(response.status_code, 403)
        # Even naming a trusted proxy does not help a peer that is not one.
        response = _get("/playlist.m3u", peer="203.0.113.7", allow="192.168.1.0/24", trusted="10.0.0.2",
                        headers={"X-Forwarded-For": "192.168.1.20"})
        self.assertEqual(response.status_code, 403)

    def test_behind_a_trusted_proxy_the_real_client_is_checked(self):
        allowed = _get("/playlist.m3u", peer="10.0.0.2", allow="192.168.1.0/24", trusted="10.0.0.2",
                       headers={"X-Forwarded-For": "192.168.1.20"})
        self.assertEqual(allowed.status_code, 200)
        refused = _get("/playlist.m3u", peer="10.0.0.2", allow="192.168.1.0/24", trusted="10.0.0.2",
                       headers={"X-Forwarded-For": "192.168.1.20, 203.0.113.7"})
        self.assertEqual(refused.status_code, 403)

    def test_dashboard_and_healthz_are_not_affected(self):
        self.assertEqual(_get("/healthz", peer="203.0.113.7", allow="192.168.1.0/24").status_code, 200)
        self.assertEqual(
            _get("/api/status", peer="203.0.113.7", allow="192.168.1.0/24", auth=("admin", "secret")).status_code,
            200,
        )


if __name__ == "__main__":
    unittest.main()
