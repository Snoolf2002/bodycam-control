"""
Bodycam Control Plane – application entry-point.

Starts:
  - FastAPI HTTP server
  - Async TCP telemetry gateway on port 6608
  - Redis + TimescaleDB connection pools
"""

import asyncio
import logging
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from app.api.dependencies import init_redis, close_redis, init_db, close_db
from app.api.routes import router as api_router
from app.core.config import settings
from app.gateway.socket_server import start_telemetry_server

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manages startup/shutdown of background services."""
    # ── Startup ──────────────────────────────────────────────────────────
    logger.info("Initialising Redis connection pool …")
    await init_redis()

    logger.info("Initialising TimescaleDB connection pool …")
    await init_db()

    logger.info("Starting telemetry gateway on %s:%d …", settings.GATEWAY_HOST, settings.GATEWAY_PORT)
    gateway_task = asyncio.create_task(
        start_telemetry_server(settings.GATEWAY_HOST, settings.GATEWAY_PORT)
    )

    yield

    # ── Shutdown ─────────────────────────────────────────────────────────
    gateway_task.cancel()
    try:
        await gateway_task
    except asyncio.CancelledError:
        pass
    await close_redis()
    await close_db()
    logger.info("Shutdown complete")


app = FastAPI(
    title="Bodycam Control Plane",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(api_router)

if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host=settings.API_HOST,
        port=settings.API_PORT,
        reload=False,
        workers=1,   # Must be 1: TCP gateway runs as asyncio task in-process
    )
