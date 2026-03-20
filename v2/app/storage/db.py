"""db.py - Async SQLite engine setup."""
from __future__ import annotations
from pathlib import Path
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from app.storage.models import Base
from app.logger import get_logger

log = get_logger(__name__)

_engine = None
_session_factory = None


async def init_db(db_path: str) -> None:
    global _engine, _session_factory
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    _engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}", echo=False)
    async with _engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    _session_factory = async_sessionmaker(_engine, expire_on_commit=False)
    log.info(f"db initialized: {db_path}")


def get_session() -> AsyncSession:
    if not _session_factory:
        raise RuntimeError("Call init_db() first")
    return _session_factory()
