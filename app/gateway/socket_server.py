"""
Production-grade async TCP telemetry gateway for JT/T 808 bodycam devices.

Features:
  - Per-connection PacketBuffer for safe TCP stream reassembly
  - Redis-backed device registry (no in-memory dict)
  - Async GPS persistence to TimescaleDB
  - Read timeout to evict stale connections
  - Proper JT/T 808 response packets sent back to devices
"""

import asyncio
import struct
import logging
from datetime import datetime, timezone
from typing import Optional

from app.gateway.protocol_808 import (
    PacketBuffer,
    ParsedPacket,
    parse_packet,
    parse_location,
    build_packet,
)
from app.api.dependencies import get_device_store, get_session_factory
from app.core.config import settings
from app.core.security import generate_stream_token
from app.models.database import GPSTrack

logger = logging.getLogger(__name__)


class DeviceConnection:
    """Manages a single persistent TCP connection from a bodycam device."""

    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ):
        self.reader = reader
        self.writer = writer
        self.addr = writer.get_extra_info("peername")
        self.device_id: Optional[str] = None
        self.phone_bcd: bytes = b'\x00' * 6
        self.buffer = PacketBuffer()

    # ── Main loop ────────────────────────────────────────────────────────

    async def handle(self) -> None:
        logger.info("New connection from %s", self.addr)
        store = get_device_store()

        try:
            while True:
                data = await asyncio.wait_for(
                    self.reader.read(4096),
                    timeout=float(settings.DEVICE_TTL_SECONDS),
                )
                if not data:
                    break

                for raw in self.buffer.feed(data):
                    try:
                        pkt = parse_packet(raw)
                        await self._dispatch(pkt, store)
                    except ValueError as exc:
                        logger.warning("[%s] Malformed packet: %s", self.addr, exc)

        except asyncio.TimeoutError:
            logger.warning("[%s] Timed out after %ds", self.addr, settings.DEVICE_TTL_SECONDS)
        except ConnectionResetError:
            logger.info("[%s] Connection reset by peer", self.addr)
        except Exception as exc:
            logger.error("[%s] Unexpected error: %s", self.addr, exc, exc_info=True)
        finally:
            await self._cleanup(store)

    # ── Dispatcher ───────────────────────────────────────────────────────

    async def _dispatch(self, pkt: ParsedPacket, store) -> None:
        # Capture device identity from the first packet received
        if self.device_id is None:
            self.device_id = pkt.phone_number
            raw_bytes = bytes.fromhex(pkt.phone_number)
            self.phone_bcd = raw_bytes.rjust(6, b'\x00')[:6]

        handlers = {
            0x0100: self._on_register,
            0x0102: self._on_auth,
            0x0002: self._on_heartbeat,
            0x0200: self._on_location,
        }
        handler = handlers.get(pkt.msg_id)
        if handler:
            await handler(pkt, store)
        else:
            logger.debug("[%s] Unhandled msg_id=%#06x", self.device_id, pkt.msg_id)
            await self._ack(pkt, result=0)

    # ── Message handlers ─────────────────────────────────────────────────

    async def _on_register(self, pkt: ParsedPacket, store) -> None:
        logger.info("[%s] Registration from %s", self.device_id, self.addr)
        await store.register_device(self.device_id, self.addr)

        # Generate HMAC stream token and persist in Redis
        token = generate_stream_token(self.device_id, settings.SECRET_KEY)
        await store.store_stream_token(self.device_id, token)

        # Reply 0x8100: seq(WORD) + result(BYTE=0 success) + auth_code
        auth_code = self.device_id.encode("ascii")
        body = struct.pack(">H", pkt.msg_seq) + b'\x00' + auth_code
        self.writer.write(build_packet(0x8100, self.phone_bcd, pkt.msg_seq, body))
        await self.writer.drain()
        logger.info("[%s] Registered successfully", self.device_id)

    async def _on_auth(self, pkt: ParsedPacket, store) -> None:
        logger.info("[%s] Authentication from %s", self.device_id, self.addr)
        await store.register_device(self.device_id, self.addr)
        await self._ack(pkt, result=0)

    async def _on_heartbeat(self, pkt: ParsedPacket, store) -> None:
        logger.debug("[%s] Heartbeat", self.device_id)
        await store.heartbeat(self.device_id)
        await self._ack(pkt, result=0)

    async def _on_location(self, pkt: ParsedPacket, store) -> None:
        try:
            loc = parse_location(pkt.body)
        except ValueError as exc:
            logger.warning("[%s] Bad location payload: %s", self.device_id, exc)
            return

        logger.info(
            "[%s] GPS lat=%.6f lon=%.6f spd=%.1fkm/h dir=%d",
            self.device_id,
            loc.latitude,
            loc.longitude,
            loc.speed,
            loc.direction,
        )

        # Persist to TimescaleDB asynchronously
        try:
            session_factory = get_session_factory()
            async with session_factory() as session:
                track = GPSTrack(
                    time=datetime.now(timezone.utc),
                    device_id=self.device_id,
                    latitude=loc.latitude,
                    longitude=loc.longitude,
                    speed=loc.speed,
                    direction=loc.direction,
                    elevation=loc.elevation,
                    alarm_flags=loc.alarm_flags,
                    status_flags=loc.status_flags,
                )
                session.add(track)
                await session.commit()
        except Exception as exc:
            logger.error("[%s] DB write failed: %s", self.device_id, exc)

        await self._ack(pkt, result=0)

    # ── Helpers ──────────────────────────────────────────────────────────

    async def _ack(self, pkt: ParsedPacket, result: int = 0) -> None:
        """Send a generic 0x8001 platform acknowledgement."""
        body = struct.pack(">HHB", pkt.msg_seq, pkt.msg_id, result)
        self.writer.write(build_packet(0x8001, self.phone_bcd, pkt.msg_seq, body))
        await self.writer.drain()

    async def _cleanup(self, store) -> None:
        logger.info("[%s] Connection closed (device=%s)", self.addr, self.device_id)
        if self.device_id:
            await store.remove_device(self.device_id)
        self.writer.close()
        try:
            await self.writer.wait_closed()
        except Exception:
            pass


# ── Server entry-point ───────────────────────────────────────────────────────

async def start_telemetry_server(host: str, port: int) -> None:
    async def _on_connect(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        conn = DeviceConnection(reader, writer)
        await conn.handle()

    server = await asyncio.start_server(_on_connect, host, port)
    addrs = ", ".join(str(s.getsockname()) for s in server.sockets)
    logger.info("Telemetry Server listening on %s", addrs)

    async with server:
        await server.serve_forever()
