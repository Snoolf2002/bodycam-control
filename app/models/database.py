"""
Database models and schema initialization for the Bodycam Control Plane.

Uses raw SQL DDL for the gps_tracks table so that:
  - The composite primary key (id, time) is guaranteed from the start.
  - TimescaleDB hypertable conversion succeeds on the first attempt.
  - If a stale single-PK table exists from an old deployment, it is
    automatically dropped and recreated with the correct schema.
"""

import logging
from datetime import datetime, timezone

from sqlalchemy import (
    Column, String, Float, Integer, BigInteger, DateTime, text, Index
)
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

logger = logging.getLogger(__name__)


class Base(DeclarativeBase):
    pass


class GPSTrack(Base):
    """Stores individual GPS coordinate reports from devices."""

    __tablename__ = "gps_tracks"

    # Composite primary key required for TimescaleDB hypertable on 'time'
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


# ── Schema init ───────────────────────────────────────────────────────────────

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS gps_tracks (
    id          BIGSERIAL,
    time        TIMESTAMPTZ   NOT NULL DEFAULT now(),
    device_id   VARCHAR(20)   NOT NULL,
    latitude    DOUBLE PRECISION NOT NULL,
    longitude   DOUBLE PRECISION NOT NULL,
    speed       DOUBLE PRECISION,
    direction   INTEGER,
    elevation   INTEGER,
    alarm_flags INTEGER,
    status_flags INTEGER,
    PRIMARY KEY (id, time)
);
"""

_CREATE_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS ix_gps_tracks_device_id
ON gps_tracks (device_id);
"""

_CHECK_PK_SQL = """
SELECT kcu.column_name
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
  ON tc.constraint_name = kcu.constraint_name
  AND tc.table_schema = kcu.table_schema
WHERE tc.constraint_type = 'PRIMARY KEY'
  AND tc.table_name = 'gps_tracks';
"""


async def _table_has_correct_schema(conn) -> bool:
    """Return True if gps_tracks has the composite PK (id, time)."""
    result = await conn.execute(text(_CHECK_PK_SQL))
    pk_cols = {row[0] for row in result.fetchall()}
    return "id" in pk_cols and "time" in pk_cols


async def init_database(engine) -> None:
    """
    Ensure gps_tracks exists with the correct composite primary key schema.

    Strategy:
      1. If the table does not exist → create it with the correct DDL.
      2. If the table exists but has the OLD single-PK schema (id only) →
         drop it and recreate it (safe: no real GPS data yet on fresh deploy).
      3. Attempt to register as a TimescaleDB hypertable (optional).
    """
    async with engine.begin() as conn:
        # Check whether the table already exists
        exists_result = await conn.execute(
            text(
                "SELECT EXISTS ("
                "  SELECT 1 FROM information_schema.tables"
                "  WHERE table_schema = 'public'"
                "  AND table_name = 'gps_tracks'"
                ");"
            )
        )
        table_exists = exists_result.scalar()

        if table_exists:
            correct = await _table_has_correct_schema(conn)
            if not correct:
                logger.warning(
                    "gps_tracks exists with wrong schema (single PK). "
                    "Dropping and recreating with composite PK (id, time)…"
                )
                await conn.execute(text("DROP TABLE IF EXISTS gps_tracks CASCADE;"))
                table_exists = False  # will be recreated below

        if not table_exists:
            await conn.execute(text(_CREATE_TABLE_SQL))
            await conn.execute(text(_CREATE_INDEX_SQL))
            logger.info("gps_tracks table created with composite PK (id, time).")
        else:
            logger.info("gps_tracks table already exists with correct schema.")

    # Attempt to convert to TimescaleDB hypertable (optional)
    try:
        async with engine.begin() as conn:
            await conn.execute(
                text(
                    "SELECT create_hypertable("
                    "  'gps_tracks', 'time',"
                    "  if_not_exists => TRUE"
                    ");"
                )
            )
            logger.info("TimescaleDB hypertable registered for gps_tracks.")
    except Exception as exc:
        logger.warning(
            "TimescaleDB hypertable creation skipped (standard table will be used): %s",
            exc,
        )
