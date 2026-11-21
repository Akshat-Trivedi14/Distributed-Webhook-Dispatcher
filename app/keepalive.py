"""Keeps Render's free tier from putting the service to sleep.

Render sleeps a free service after ~15 minutes without incoming requests. Render also tells every service
its own public address (the RENDER_EXTERNAL_URL variable), so we simply request our own /health now and
then. This is a backup: an outside monitor such as UptimeRobot is still needed to wake a sleeping service.
"""
from __future__ import annotations

import asyncio
import logging

import httpx

from .logging_setup import log

logger = logging.getLogger("keepalive")


async def keep_awake(base_url: str, interval_s: float, client: httpx.AsyncClient | None = None) -> None:
    """Request `<base_url>/health` every `interval_s` seconds, forever. A failed ping never stops the loop."""
    own_client = client is None
    client = client or httpx.AsyncClient(timeout=15)
    try:
        while True:
            await asyncio.sleep(interval_s)
            try:
                response = await client.get(f"{base_url.rstrip('/')}/health")
                log(logger, "keepalive_ping", status=response.status_code)
            except Exception as exc:
                log(logger, "keepalive_failed", level=logging.WARNING, error=str(exc))
    finally:
        if own_client:
            await client.aclose()
