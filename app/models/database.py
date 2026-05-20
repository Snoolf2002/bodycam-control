from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import Column, String, Float, Integer, DateTime, BigInteger, text
from datetime import datetime, timezone


class Base(DeclarativeBase):
    pass


class GPSTrack(Base):
    """Stores individual GPS coordinate reports from devices."""

    __tablename__ = "gps_tracks"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    time = Column(
        DateTime(timezone=True),
        primary_key=True,
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    device_id = Column(String(20), nullable=False, index=True)
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    speed = Column(Float, nullable=True)
    direction = Column(Integer, nullable=True)
    elevation = Column(Integer, nullable=True)
    alarm_flags = Column(Integer, nullable=True)
    status_flags = Column(Integer, nullable=True)


import logging

logger = logging.getLogger(__name__)


async def init_database(engine) -> None:
    """Create tables and optionally convert to TimescaleDB hypertable."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        logger.info("Database tables created successfully.")

    try:
        async with engine.begin() as conn:
            await conn.execute(
                text(
                    "SELECT create_hypertable('gps_tracks', 'time', "
                    "if_not_exists => TRUE)"
                )
            )
            logger.info("TimescaleDB hypertable created successfully.")
    except Exception as exc:
        # TimescaleDB extension may not be available; regular table is fine
        logger.warning(
            "TimescaleDB extension or hypertable creation failed (falling back to standard table): %s",
            exc,
        )

