"""Optional per-app DNS: resolve internet hostnames through resolvers you
choose instead of the system's.

For a network whose DNS filter (Pi-hole, router blocklist, ...) blocks a
provider domain that you want Jellyball, and only Jellyball, to reach. Off
unless configured:

    DNS_SERVERS=8.8.8.8,9.9.9.9                   plain DNS (UDP/TCP 53)
    DNS_OVER_HTTPS=https://dns.google/dns-query    DNS over HTTPS (port 443),
                                                   for networks that redirect
                                                   or block outbound DNS

DNS_OVER_HTTPS wins when both are set. Local names keep using the system
resolver, so the Jellyfin server and other LAN hosts resolve as before:
IP literals, "localhost", single-label names, and the suffixes in
LOCAL_SUFFIXES. When the chosen resolvers fail, the system resolver is used.

Scope: this covers every lookup inside the Jellyball process (provider pages
fetched over HTTP, stream checks, playback segment fetches, and the SSRF
check, which resolves the same way so its answer matches the connection).
The headless Chromium and ffmpeg run as separate programs and keep using the
system resolver.
"""

import ipaddress
import os
import socket
import threading
import time
import urllib.parse
from typing import Any, Dict, List, Optional, Tuple

LOCAL_SUFFIXES = (".local", ".lan", ".home", ".internal", ".localhost", ".arpa", ".home.arpa", ".localdomain")
CACHE_MIN_SECONDS = 30.0
CACHE_MAX_SECONDS = 300.0
QUERY_TIMEOUT_SECONDS = 3.0

_original_getaddrinfo = socket.getaddrinfo
_installed = False
_lock = threading.Lock()
_cache: Dict[str, Tuple[float, List[str]]] = {}
_settings: Dict[str, Any] = {"servers": [], "doh": "", "doh_host": ""}


def _parse_servers(raw: str) -> List[str]:
    servers = []
    for item in (raw or "").replace(";", ",").split(","):
        item = item.strip()
        if not item:
            continue
        try:
            servers.append(str(ipaddress.ip_address(item)))
        except ValueError:
            continue  # a resolver must be an IP address (resolving it would need DNS)
    return servers


def configure(servers: str = "", doh_url: str = "") -> bool:
    """Set the resolvers; returns True when an override is active."""
    doh = (doh_url or "").strip()
    if doh and urllib.parse.urlsplit(doh).scheme != "https":
        doh = ""
    _settings["servers"] = _parse_servers(servers)
    _settings["doh"] = doh
    _settings["doh_host"] = (urllib.parse.urlsplit(doh).hostname or "").lower() if doh else ""
    with _lock:
        _cache.clear()
    return active()


def active() -> bool:
    return bool(_settings["doh"] or _settings["servers"])


def describe() -> str:
    if _settings["doh"]:
        return f"DNS over HTTPS ({_settings['doh_host']})"
    if _settings["servers"]:
        return "DNS servers " + ", ".join(_settings["servers"])
    return "system resolver"


def _is_local_name(host: str) -> bool:
    name = host.rstrip(".").lower()
    if not name or name == "localhost" or "." not in name:
        return True
    try:
        ipaddress.ip_address(name.strip("[]"))
        return True
    except ValueError:
        pass
    if name == _settings["doh_host"]:
        return True  # the DoH server itself must resolve the normal way
    return name.endswith(LOCAL_SUFFIXES)


_doh_client: Any = None


def _doh_post(url: str, wire: bytes) -> bytes:
    """One RFC 8484 query (POST application/dns-message) with the httpx the app
    already ships. The DoH server's own name resolves through the system (see
    _is_local_name): allow that one name in the local filter if it blocks it."""
    global _doh_client
    import httpx

    if _doh_client is None:
        _doh_client = httpx.Client(timeout=QUERY_TIMEOUT_SECONDS, follow_redirects=False)
    response = _doh_client.post(
        url, content=wire,
        headers={"content-type": "application/dns-message", "accept": "application/dns-message"},
    )
    response.raise_for_status()
    return response.content


def _query(name: str, rdtype: str) -> Tuple[List[str], float]:
    import dns.message
    import dns.query
    import dns.flags
    import dns.rdatatype

    query = dns.message.make_query(name, rdtype)
    if _settings["doh"]:
        query.id = 0  # RFC 8484 recommends 0 for cache friendliness
        response = dns.message.from_wire(_doh_post(_settings["doh"], query.to_wire()))
    else:
        response = None
        last_error: Optional[BaseException] = None
        for server in _settings["servers"]:
            try:
                response = dns.query.udp(query, server, timeout=QUERY_TIMEOUT_SECONDS)
                if response.flags & dns.flags.TC:  # truncated: retry over TCP
                    response = dns.query.tcp(query, server, timeout=QUERY_TIMEOUT_SECONDS)
                break
            except Exception as exc:  # noqa: BLE001 - try the next server
                last_error = exc
        if response is None:
            raise last_error or OSError("no DNS server answered")
    wanted = dns.rdatatype.from_text(rdtype)
    addresses: List[str] = []
    ttl = CACHE_MAX_SECONDS
    for rrset in response.answer:
        if rrset.rdtype == wanted:
            ttl = min(ttl, float(rrset.ttl))
            addresses.extend(item.to_text() for item in rrset)
    return addresses, ttl


def resolve(host: str) -> List[str]:
    """IPv4 then IPv6 addresses for host through the configured resolvers (cached)."""
    name = host.rstrip(".").lower()
    now = time.monotonic()
    with _lock:
        cached = _cache.get(name)
        if cached and cached[0] > now:
            return list(cached[1])
    addresses: List[str] = []
    ttls: List[float] = []
    for rdtype in ("A", "AAAA"):
        try:
            found, ttl = _query(name, rdtype)
        except Exception:  # noqa: BLE001 - one record type failing is not fatal
            continue
        addresses.extend(found)
        if found:
            ttls.append(ttl)
    if addresses:
        lifetime = max(CACHE_MIN_SECONDS, min(min(ttls), CACHE_MAX_SECONDS))
        with _lock:
            _cache[name] = (now + lifetime, list(addresses))
    return addresses


def _getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
    if not active() or not isinstance(host, str) or _is_local_name(host):
        return _original_getaddrinfo(host, port, family, type, proto, flags)
    addresses = resolve(host)
    if not addresses:
        # The chosen resolvers did not answer: fall back to the system so a
        # resolver outage does not take every provider down.
        return _original_getaddrinfo(host, port, family, type, proto, flags)
    results = []
    for address in addresses:
        try:
            results.extend(_original_getaddrinfo(address, port, family, type, proto, flags | socket.AI_NUMERICHOST))
        except socket.gaierror:
            continue  # e.g. an IPv6 answer when only IPv4 was asked for
    if not results:
        return _original_getaddrinfo(host, port, family, type, proto, flags)
    return results


def install_from_env() -> bool:
    """Read DNS_SERVERS / DNS_OVER_HTTPS and hook the process resolver when set."""
    global _installed
    if not configure(os.getenv("DNS_SERVERS", ""), os.getenv("DNS_OVER_HTTPS", "")):
        return False
    if not _installed:
        socket.getaddrinfo = _getaddrinfo
        _installed = True
    return True


def uninstall() -> None:
    """Restore the system resolver (tests)."""
    global _installed
    socket.getaddrinfo = _original_getaddrinfo
    _installed = False
    configure("", "")
