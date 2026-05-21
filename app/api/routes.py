"""
API routes for the Bodycam Control Plane.

Endpoints:
  POST /webhook/rtsp_auth  – MediaMTX external authentication webhook
  GET  /devices             – List all currently connected devices
  GET  /devices/{id}/token  – Get the HMAC stream token for a device
  GET  /devices/{id}/location – Get latest GPS location for a device
"""

import base64
import logging
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.api.dependencies import get_device_store, get_session_factory
from app.core.config import settings
from app.core.security import verify_stream_token
from app.models.database import GPSTrack
from sqlalchemy import select

router = APIRouter()
logger = logging.getLogger(__name__)


# ── Request / Response schemas ───────────────────────────────────────────────

class MediaMTXAuthRequest(BaseModel):
    ip: str = ""
    user: str = ""
    password: str = ""
    path: str = ""
    protocol: str = ""
    id: str = ""
    action: str = ""
    query: str = ""


class DeviceInfo(BaseModel):
    device_id: str
    address: str
    registered_at: float
    last_heartbeat: float


class LocationInfo(BaseModel):
    device_id: str
    latitude: float
    longitude: float
    speed: Optional[float] = None
    direction: Optional[int] = None
    elevation: Optional[int] = None
    time: str


# ── Endpoints ────────────────────────────────────────────────────────────────

@router.post("/webhook/rtsp_auth")
async def rtsp_auth(request: MediaMTXAuthRequest):
    """
    MediaMTX external authentication webhook.

    Validates the RTSP path token in two modes:
      1. HMAC token (issued by our platform) – cryptographically verified.
      2. Legacy CMSv6 token (Base64 CSV with device ID) – checked against
         the Redis active-device registry.
    """
    path = request.path.strip("/")
    if not path:
        raise HTTPException(status_code=401, detail="Empty path")

    store = get_device_store()

    # ── Try HMAC token first ─────────────────────────────────────────────
    device_id = verify_stream_token(path, settings.SECRET_KEY)
    if device_id:
        if await store.is_online(device_id):
            logger.info("HMAC auth OK for device %s", device_id)
            return {"status": "ok"}
        raise HTTPException(status_code=401, detail="Device offline")

    # ── Fallback: legacy Base64 CMSv6 token ──────────────────────────────
    try:
        decoded = base64.b64decode(path).decode("utf-8")
        parts = decoded.split(",")
        if len(parts) >= 3:
            device_id = parts[2]
            if await store.is_online(device_id):
                logger.info("Legacy token auth OK for device %s", device_id)
                return {"status": "ok"}
            raise HTTPException(status_code=401, detail="Device offline")
    except Exception:
        pass

    logger.warning("Auth rejected for path=%s", path[:60])
    raise HTTPException(status_code=401, detail="Invalid token")


@router.get("/devices")
async def list_devices():
    """List all currently connected devices with connection metadata."""
    store = get_device_store()
    devices = await store.get_all_devices()
    return {"connected_devices": devices, "count": len(devices)}


@router.get("/devices/{device_id}/token")
async def get_device_token(device_id: str):
    """Retrieve the current HMAC stream token for a connected device."""
    store = get_device_store()
    if not await store.is_online(device_id):
        raise HTTPException(status_code=404, detail="Device not connected")
    token = await store.get_stream_token(device_id)
    if not token:
        raise HTTPException(status_code=404, detail="No token available")
    return {"device_id": device_id, "stream_token": token}


@router.get("/devices/{device_id}/location")
async def get_device_location(device_id: str):
    """Get the most recent GPS coordinate for a device from TimescaleDB."""
    try:
        session_factory = get_session_factory()
        async with session_factory() as session:
            result = await session.execute(
                select(GPSTrack)
                .where(GPSTrack.device_id == device_id)
                .order_by(GPSTrack.time.desc())
                .limit(1)
            )
            track = result.scalar_one_or_none()
            if not track:
                raise HTTPException(status_code=404, detail="No location data yet")
            return LocationInfo(
                device_id=track.device_id,
                latitude=track.latitude,
                longitude=track.longitude,
                speed=track.speed,
                direction=track.direction,
                elevation=track.elevation,
                time=track.time.isoformat(),
            )
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Location DB error for device %s: %s", device_id, exc)
        raise HTTPException(status_code=404, detail="No location data yet")


@router.get("/diagnose")
async def diagnose_connectivity():
    """Diagnostic endpoint to inspect container DNS and TCP connectivity."""
    import socket
    import asyncio

    results = {}


    # Test DNS resolution
    for host in ["app", "mediamtx", "redis", "timescaledb"]:
        try:
            ips = socket.gethostbyname_ex(host)
            results[f"dns_{host}"] = {"status": "ok", "ip": ips[2]}
        except Exception as e:
            results[f"dns_{host}"] = {"status": "error", "message": str(e)}

    # Test TCP connections
    tcp_targets = [
        ("mediamtx", 8888),
        ("mediamtx", 8889),
        ("app", 8001),
        ("redis", 6379),
    ]

    for host, port in tcp_targets:
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port), timeout=2.0
            )
            writer.close()
            await writer.wait_closed()
            results[f"tcp_{host}_{port}"] = {"status": "ok"}
        except Exception as e:
            results[f"tcp_{host}_{port}"] = {"status": "error", "message": str(e)}

    return results
