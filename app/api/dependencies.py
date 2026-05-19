from typing import Optional
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import (
    create_async_engine,
    AsyncSession,
    async_sessionmaker,
    AsyncEngine,
)
from app.core.config import settings
from app.services.redis_store import DeviceStore
from app.models.database import init_database

import logging
import asyncio

logger = logging.getLogger(__name__)

# ── Global singletons (initialised at app startup) ──────────────────────────

_redis: Optional[Redis] = None
_engine: Optional[AsyncEngine] = None
_session_factory: Optional[async_sessionmaker[AsyncSession]] = None
_device_store: Optional[DeviceStore] = None


# ── Lifecycle helpers ────────────────────────────────────────────────────────

async def init_redis() -> None:
    global _redis, _device_store
    _redis = Redis.from_url(settings.REDIS_URL, decode_responses=False)
    _device_store = DeviceStore(_redis, ttl=settings.DEVICE_TTL_SECONDS)
    
    retries = 10
    for i in range(retries):
        try:
            await _redis.ping()
            logger.info("Successfully connected to Redis.")
            return
        except Exception as exc:
            logger.warning(
                f"Redis connection attempt {i+1}/{retries} failed: {exc}. Retrying in 3 seconds..."
            )
            if i == retries - 1:
                raise exc
            await asyncio.sleep(3)


async def close_redis() -> None:
    global _redis
    if _redis:
        await _redis.close()


async def init_db() -> None:
    global _engine, _session_factory
    _engine = create_async_engine(
        settings.DATABASE_URL, pool_size=20, max_overflow=10
    )
    _session_factory = async_sessionmaker(_engine, expire_on_commit=False)
    
    retries = 10
    for i in range(retries):
        try:
            await init_database(_engine)
            logger.info("Successfully connected and initialized TimescaleDB.")
            return
        except Exception as exc:
            logger.warning(
                f"Database connection attempt {i+1}/{retries} failed: {exc}. Retrying in 3 seconds..."
            )
            if i == retries - 1:
                raise exc
            await asyncio.sleep(3)


async def close_db() -> None:
    global _engine
    if _engine:
        await _engine.dispose()


# ── Accessors (used by routes / gateway) ─────────────────────────────────────

def get_redis() -> Redis:
    assert _redis is not None, "Redis not initialised"
    return _redis


def get_device_store() -> DeviceStore:
    assert _device_store is not None, "DeviceStore not initialised"
    return _device_store


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    assert _session_factory is not None, "Database not initialised"
    return _session_factory
