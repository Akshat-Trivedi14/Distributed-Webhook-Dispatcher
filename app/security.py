"""SSRF protection: stop users from pointing webhooks at our own internal network.

Two checks:
  1. `validate_destination_url` runs when a job is submitted and rejects obviously bad URLs (400).
  2. `check_host_is_public` runs right before every delivery. It looks up the hostname's real IP
     addresses and refuses private ones, which catches tricks like a public-looking domain that
     resolves to 127.0.0.1 or to the cloud metadata address 169.254.169.254.
"""
from __future__ import annotations

import asyncio
import ipaddress
import socket
from urllib.parse import urlsplit

_BLOCKED_HOST_SUFFIXES = (".localhost", ".local", ".internal", ".lan", ".home.arpa")
MAX_URL_LENGTH = 2048


class BlockedDestination(Exception):
    """Destination resolves to a non-public address (never retried)."""


def _ip_blocked(ip: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    if isinstance(ip, ipaddress.IPv6Address) and ip.ipv4_mapped:
        ip = ip.ipv4_mapped
    # not is_global covers loopback, private, link-local (169.254.169.254 metadata), CGNAT, etc.
    return (not ip.is_global) or ip.is_multicast


def validate_destination_url(raw: str, *, allow_private: bool) -> tuple[str, str]:
    """Return (normalised_url, lowercase_hostname) or raise ValueError with a user-facing message."""
    url = raw.strip()
    if not url:
        raise ValueError("destinationUrl must not be empty")
    if len(url) > MAX_URL_LENGTH:
        raise ValueError(f"destinationUrl must be at most {MAX_URL_LENGTH} characters")
    try:
        parts = urlsplit(url)
        _ = parts.port  # raises ValueError on a malformed/out-of-range port
    except ValueError:
        raise ValueError("destinationUrl is not a valid URL") from None
    if parts.scheme not in ("http", "https"):
        raise ValueError("destinationUrl must use the http or https scheme")
    host = (parts.hostname or "").lower().rstrip(".")
    if not host:
        raise ValueError("destinationUrl must include a hostname")
    if parts.username or parts.password:
        raise ValueError("destinationUrl must not contain credentials")
    if not allow_private:
        if host == "localhost" or host.endswith(_BLOCKED_HOST_SUFFIXES):
            raise ValueError("destinationUrl must not point to a local or internal host")
        try:
            ip = ipaddress.ip_address(host)
        except ValueError:
            pass  # a DNS name; checked again at connect time
        else:
            if _ip_blocked(ip):
                raise ValueError("destinationUrl must not point to a private or reserved IP address")
    return url, host


async def check_host_is_public(host: str, port: int, *, allow_private: bool) -> None:
    """Resolve `host` and raise BlockedDestination if any address is private/reserved."""
    if allow_private:
        return
    try:
        infos = await asyncio.get_running_loop().getaddrinfo(host, port, type=socket.SOCK_STREAM)
    except socket.gaierror as exc:
        raise ConnectionError(f"DNS lookup failed for {host}: {exc.strerror or exc}") from exc
    for info in infos:
        ip = ipaddress.ip_address(info[4][0].split("%")[0])
        if _ip_blocked(ip):
            raise BlockedDestination(f"{host} resolves to a private or reserved address")
