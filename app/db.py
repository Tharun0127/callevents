from collections.abc import AsyncIterator, Iterator
from contextlib import contextmanager

from sqlalchemy import Engine, create_engine
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import Session, sessionmaker

from app.config import get_settings

_async_engine: AsyncEngine | None = None
_async_sessionmaker: async_sessionmaker[AsyncSession] | None = None
_sync_engine: Engine | None = None
_sync_sessionmaker: sessionmaker[Session] | None = None


def get_async_engine() -> AsyncEngine:
    global _async_engine, _async_sessionmaker
    if _async_engine is None:
        s = get_settings()
        _async_engine = create_async_engine(
            s.async_database_url,
            pool_size=s.db_pool_size,
            max_overflow=s.db_max_overflow,
            pool_pre_ping=True,
        )
        _async_sessionmaker = async_sessionmaker(_async_engine, expire_on_commit=False)
    return _async_engine


def async_session_factory() -> async_sessionmaker[AsyncSession]:
    get_async_engine()
    assert _async_sessionmaker is not None
    return _async_sessionmaker


async def get_session() -> AsyncIterator[AsyncSession]:
    """FastAPI dependency: one session per request."""
    async with async_session_factory()() as session:
        yield session


async def dispose_async_engine() -> None:
    global _async_engine, _async_sessionmaker
    if _async_engine is not None:
        await _async_engine.dispose()
    _async_engine = None
    _async_sessionmaker = None


def get_sync_engine() -> Engine:
    global _sync_engine, _sync_sessionmaker
    if _sync_engine is None:
        s = get_settings()
        _sync_engine = create_engine(
            s.sync_database_url,
            pool_size=s.db_pool_size,
            max_overflow=s.db_max_overflow,
            pool_pre_ping=True,
        )
        _sync_sessionmaker = sessionmaker(_sync_engine, expire_on_commit=False)
    return _sync_engine


@contextmanager
def sync_session() -> Iterator[Session]:
    """Worker side session; commits on success, rolls back on error."""
    get_sync_engine()
    assert _sync_sessionmaker is not None
    session = _sync_sessionmaker()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def dispose_sync_engine() -> None:
    global _sync_engine, _sync_sessionmaker
    if _sync_engine is not None:
        _sync_engine.dispose()
    _sync_engine = None
    _sync_sessionmaker = None
