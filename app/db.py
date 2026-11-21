"""Database engine, session factory and table creation."""
from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from .config import Settings
from .models import Base

_SCHEMA_LOCK_ID = 727_001  # arbitrary number identifying our "creating tables" lock


def to_async_url(url: str) -> str:
    """Hosting providers hand out `postgres://` or `postgresql://`; SQLAlchemy needs the driver named."""
    for prefix in ("postgres://", "postgresql://"):
        if url.startswith(prefix):
            return "postgresql+asyncpg://" + url[len(prefix):]
    return url


def create_db(settings: Settings) -> tuple[AsyncEngine, async_sessionmaker[AsyncSession]]:
    engine = create_async_engine(to_async_url(settings.database_url), pool_size=settings.db_pool_size, max_overflow=0)
    # expire_on_commit=False keeps loaded objects readable after the session ends.
    return engine, async_sessionmaker(engine, expire_on_commit=False)


async def create_tables(engine: AsyncEngine) -> None:
    """Create any missing tables. The advisory lock stops two instances starting together from racing."""
    async with engine.begin() as conn:
        await conn.execute(text("SELECT pg_advisory_xact_lock(:id)"), {"id": _SCHEMA_LOCK_ID})
        await conn.run_sync(Base.metadata.create_all)
