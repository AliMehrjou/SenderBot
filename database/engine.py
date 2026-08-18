import logging
import contextlib
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
    AsyncEngine
)
from sqlalchemy import select
from redis.asyncio import Redis, ConnectionPool

from config import config
from .base import Base
from .models import GlobalSettings

logger = logging.getLogger(__name__)

engine: AsyncEngine = create_async_engine(
    url=config.MYSQL_URL,
    echo=False,
    pool_pre_ping=True,
    pool_recycle=3600,
    pool_size=20,
    max_overflow=10
)

async_session = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False
)


async def init_db() -> None:
    # ساخت جداول
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        
    # تزریق تنظیمات پیش‌فرض در صورت خالی بودن جدول
    async with async_session() as session:
        stmt = select(GlobalSettings).limit(1)
        result = await session.execute(stmt)
        if not result.scalar_one_or_none():
            default_settings = GlobalSettings(
                max_accounts_per_api=5,
                send_limit_per_run=40,
                cooldown_hours=24,
                spam_penalty_days=3
            )
            session.add(default_settings)
            await session.commit()
            logger.info("Default GlobalSettings initialized in the database.")


@contextlib.asynccontextmanager
async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Async context manager for database sessions.
    Commit has been removed. Handlers/Middlewares are responsible for commits.
    Safely rolls back on exception and closes upon exit.
    """
    session: AsyncSession = async_session()
    try:
        yield session
    except Exception:
        await session.rollback()
        raise
    finally:
        await session.close()

# Redis Configuration ...
redis_pool: ConnectionPool = ConnectionPool.from_url(
    url=config.REDIS_URL, decode_responses=True, max_connections=50
)

@contextlib.asynccontextmanager
async def get_redis_client() -> AsyncGenerator[Redis, None]:
    client = Redis(connection_pool=redis_pool)
    try:
        yield client
    finally:
        await client.aclose()