"""Database engine, session factory and table creation."""
from __future__ import annotations

import asyncio
import logging

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from .config import Settings
from .logging_setup import log
from .models import Base

logger = logging.getLogger("db")

_SCHEMA_LOCK_ID = 727_001  # arbitrary number identifying our "creating tables" lock


def to_async_url(url: str) -> str:
    """Hosting providers hand out `postgres://` or `postgresql://`; SQLAlchemy needs the driver named."""
    for prefix in ("postgres://", "postgresql://"):
        if url.startswith(prefix):
            return "postgresql+asyncpg://" + url[len(prefix):]
    return url


def create_db(settings: Settings) -> tuple[AsyncEngine, async_sessionmaker[AsyncSession]]:
    engine = create_async_engine(
        to_async_url(settings.database_url),
        pool_size=settings.db_pool_size,
        max_overflow=0,
        pool_pre_ping=True,  # test a connection before using it, so one the database dropped is replaced
        pool_recycle=1800,  # and don't keep any connection for more than 30 minutes
    )
    # expire_on_commit=False keeps loaded objects readable after the session ends.
    return engine, async_sessionmaker(engine, expire_on_commit=False)


async def create_tables(engine: AsyncEngine, attempts: int = 10, delay_s: float = 3.0) -> None:
    """Create any missing tables, waiting for the database if it isn't reachable yet.

    The advisory lock stops two instances starting together from racing each other.
    """
    for attempt in range(1, attempts + 1):
        try:
            async with engine.begin() as conn:
                await conn.execute(text("SELECT pg_advisory_xact_lock(:id)"), {"id": _SCHEMA_LOCK_ID})
                await conn.run_sync(Base.metadata.create_all)
            return
        except (SQLAlchemyError, OSError) as exc:
            if attempt == attempts:
                raise
            log(logger, "db_not_ready", level=logging.WARNING, attempt=attempt, error=str(exc)[:200])
            await asyncio.sleep(delay_s)
