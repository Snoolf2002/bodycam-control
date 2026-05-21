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


class StartStreamRequest(BaseModel):
    ip: str
    port: int = 6604
    channel: int = 0
    data_type: int = 0  # 0: Audio/Video, 1: Video, 2: Voice, etc.
    stream_type: int = 0  # 0: Main stream, 1: Sub-stream


# ── Endpoints ────────────────────────────────────────────────────────────────

@router.post("/webhook/rtsp_auth")
async def rtsp_auth(request: MediaMTXAuthRequest):
    """
    MediaMTX external authentication webhook.

    Called for both publish (camera pushing RTSP to port 6604) and read
    (browser/HLS client requesting the stream).

    Path formats accepted:
      1. Base64 CMSv6 path  – the format cameras use when pushing RTSP directly
         to MediaMTX (OEJGNkRF... → SESSION_TOKEN,3,device_id,...).
      2. HMAC dot-token     – issued by our platform for future pull-based use.
    """
    path = request.path.strip("/")
    action = request.action  # "publish" or "read"

    if not path:
        raise HTTPException(status_code=401, detail="Empty path")

    store = get_device_store()

    # ── 1. Base64 CMSv6 path (camera native push format) ────────────────────
    try:
        # Base64 decode; pad to multiple of 4 if needed
        padded = path + "=" * (-len(path) % 4)
        decoded = base64.b64decode(padded).decode("utf-8")
        parts = decoded.split(",")
        if len(parts) >= 3:
            device_id = parts[2].strip()
            if action == "publish":
                # Camera is publishing its own stream — always allow.
                # (Device may not be in Redis yet when the RTSP push arrives.)
                logger.info("Base64 publish auth OK for device %s", device_id)
                return {"status": "ok"}
            else:
                # Browser/HLS read — require device to be currently online.
                if await store.is_online(device_id):
                    logger.info("Base64 read auth OK for device %s", device_id)
                    return {"status": "ok"}
                logger.warning("Base64 read rejected — device %s offline", device_id)
                raise HTTPException(status_code=401, detail="Device offline")
    except HTTPException:
        raise
    except Exception:
        pass  # Not a valid base64 path, fall through

    # ── 2. HMAC dot-token (platform-issued, for pull-based compatibility) ────
    device_id = verify_stream_token(path, settings.SECRET_KEY)
    if device_id:
        if await store.is_online(device_id):
            logger.info("HMAC auth OK for device %s (action=%s)", device_id, action)
            return {"status": "ok"}
        raise HTTPException(status_code=401, detail="Device offline")

    logger.warning("Auth rejected for path=%.60s action=%s", path, action)
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


@router.post("/devices/{device_id}/start-stream")
async def start_device_stream(device_id: str, request: StartStreamRequest):
    """
    Send JT/T 1078 0x9101 Real-time Audio/Video Transmission Request to the device.
    """
    from app.gateway.socket_server import active_connections
    from app.gateway.protocol_808 import build_9101_body

    conn = active_connections.get(device_id)
    if not conn:
        raise HTTPException(
            status_code=404,
            detail=f"Device connection not found or offline: {device_id}",
        )

    if getattr(conn, "is_ascii", False):
        try:
            packet_str = await conn.send_ascii_command(
                "9101",
                [
                    request.ip,
                    request.port,
                    request.channel,
                    request.data_type,
                    request.stream_type,
                ]
            )
            logger.info(
                "[%s] Dispatched ASCII 9101 command: %s",
                device_id,
                packet_str,
            )
            return {
                "status": "ok",
                "message": "Start-stream ASCII command sent successfully",
                "device_id": device_id,
                "packet": packet_str,
            }
        except Exception as exc:
            logger.error("[%s] Failed to send ASCII 9101 command: %s", device_id, exc)
            raise HTTPException(
                status_code=500,
                detail=f"Failed to transmit ASCII command to the device: {exc}",
            )

    try:
        body = build_9101_body(
            ip=request.ip,
            tcp_port=request.port,
            udp_port=0,  # TCP mode
            channel=request.channel,
            data_type=request.data_type,
            stream_type=request.stream_type,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=400,
            detail=f"Failed to build 0x9101 body payload: {exc}",
        )

    try:
        seq = await conn.send_command(0x9101, body)
        logger.info(
            "[%s] Dispatched 0x9101 command (seq=%d, ip=%s, port=%d)",
            device_id,
            seq,
            request.ip,
            request.port,
        )
        return {
            "status": "ok",
            "message": "Start-stream command sent successfully",
            "device_id": device_id,
            "msg_seq": seq,
        }
    except Exception as exc:
        logger.error("[%s] Failed to send 0x9101 command: %s", device_id, exc)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to transmit command to the device: {exc}",
        )

