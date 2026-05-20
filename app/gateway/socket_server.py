"""
Production-grade async TCP telemetry gateway for JT/T 808 bodycam devices.

Features:
  - Per-connection PacketBuffer for safe TCP stream reassembly
  - Redis-backed device registry (no in-memory dict)
  - Async GPS persistence to TimescaleDB
  - Read timeout to evict stale connections
  - Proper JT/T 808 response packets sent back to devices
  - Bidirectional RTSP proxy for camera video streaming
"""

import asyncio
import base64
import struct
import logging
from datetime import datetime, timezone
from typing import Optional, Dict

from app.gateway.protocol_808 import (
    PacketBuffer,
    ParsedPacket,
    parse_packet,
    parse_location,
    build_packet,
)
from app.api.dependencies import get_device_store, get_session_factory
from app.core.config import settings
from app.core.security import generate_stream_token, verify_stream_token
from app.models.database import GPSTrack

logger = logging.getLogger(__name__)

# Global registry: device_id -> DeviceConnection (keeps the live socket accessible to the proxy)
active_connections: Dict[str, "DeviceConnection"] = {}

_SESSION_TOKEN = "8BF6DE248647478581A01D6A42B2E452"


def generate_dynamic_rtsp_path(device_id: str) -> str:
    """Generate the Base64 RTSP path the camera firmware expects."""
    raw_payload = f"{_SESSION_TOKEN},3,{device_id},0,1,0,0,0"
    return base64.b64encode(raw_payload.encode("utf-8")).decode("utf-8")


def parse_ascii_location(segments: list[str]) -> Optional[dict]:
    """Parse lat, lon, speed, direction, elevation from ASCII CSV segments."""
    if len(segments) < 6:
        return None
    try:
        lat = float(segments[4])
        lon = float(segments[5])
        
        # Validate latitude (-90 to 90) and longitude (-180 to 180)
        if -90 <= lat <= 90 and -180 <= lon <= 180 and (lat != 0.0 or lon != 0.0):
            speed = 0.0
            if len(segments) > 6:
                try:
                    speed = float(segments[6])
                except ValueError:
                    pass
            
            direction = 0
            if len(segments) > 7:
                try:
                    direction = int(float(segments[7]))
                except ValueError:
                    pass
                    
            elevation = 0
            if len(segments) > 8:
                try:
                    elevation = int(float(segments[8]))
                except ValueError:
                    pass
            
            return {
                "latitude": lat,
                "longitude": lon,
                "speed": speed,
                "direction": direction,
                "elevation": elevation
            }
    except ValueError:
        pass
    return None


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
        self.b64_path: Optional[str] = None    # RTSP path the camera expects
        self.proxying: bool = False             # True when proxy has taken over the socket
        self.handle_task: Optional[asyncio.Task] = None  # Reference for cancellation

    # ── Main loop ────────────────────────────────────────────────────────

    async def handle(self) -> None:
        logger.info("New connection from %s", self.addr)
        store = get_device_store()
        self.handle_task = asyncio.current_task()

        try:
            # Read first chunk to determine protocol (ASCII or JT/T 808 Binary)
            first_chunk = await asyncio.wait_for(
                self.reader.read(4096),
                timeout=10.0,
            )
            if not first_chunk:
                return

            # Check for ASCII marker
            is_ascii = b"$$" in first_chunk or (first_chunk.startswith(b"$") and b"#" in first_chunk)

            if is_ascii:
                logger.info("[%s] Detected ASCII protocol connection", self.addr)
                await self.handle_ascii(first_chunk, store)
            else:
                logger.info("[%s] Detected Binary JT/T 808 protocol connection", self.addr)
                await self.handle_binary(first_chunk, store)

        except asyncio.CancelledError:
            # Proxy server cancelled the telemetry task to take ownership of the socket
            if not self.proxying:
                logger.info("[%s] Telemetry task cancelled (device=%s)", self.addr, self.device_id)
        except asyncio.TimeoutError:
            logger.warning("[%s] Timed out waiting for initial packet", self.addr)
        except ConnectionResetError:
            logger.info("[%s] Connection reset by peer", self.addr)
        except Exception as exc:
            logger.error("[%s] Unexpected error: %s", self.addr, exc, exc_info=True)
        finally:
            await self._cleanup(store)

    async def handle_binary(self, first_chunk: bytes, store) -> None:
        # Feed the first chunk
        for raw in self.buffer.feed(first_chunk):
            try:
                pkt = parse_packet(raw)
                await self._dispatch(pkt, store)
            except ValueError as exc:
                logger.warning("[%s] Malformed packet: %s", self.addr, exc)

        # Read loop
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

    async def handle_ascii(self, first_chunk: bytes, store) -> None:
        ascii_buffer = ""
        
        # Helper to process a single complete ASCII packet (e.g. $$...,...,...#)
        async def process_ascii_packet(packet_str: str) -> None:
            if not ("$$" in packet_str and "#" in packet_str):
                return
            
            # Extract content between $$ and #
            content = packet_str.split('$$')[-1].split('#')[0]
            segments = [s.strip() for s in content.split(',')]
            
            # Debug: log the raw packet so we can see exactly what the camera sends
            logger.debug("[%s] RAW ASCII packet: %s", self.addr, packet_str[:200])
            logger.debug("[%s] Parsed segments (%d): %s", self.addr, len(segments), segments)
            
            if len(segments) >= 4:
                device_id = segments[3]
                if not device_id:
                    return
                
                # If device ID changes or is first registered
                if self.device_id != device_id:
                    self.device_id = device_id
                    self.b64_path = generate_dynamic_rtsp_path(device_id)
                    active_connections[device_id] = self
                    logger.info("[%s] ASCII Device identified: %s (path=%s...)", self.addr, self.device_id, self.b64_path[:16])
                    await store.register_device(self.device_id, self.addr)
                    
                    # Generate and store stream token so they can watch HLS/WebRTC
                    token = generate_stream_token(self.device_id, settings.SECRET_KEY)
                    await store.store_stream_token(self.device_id, token)
                    logger.info("[%s] ASCII Device registered successfully with stream token", self.device_id)
                else:
                    # Update heartbeat
                    await store.heartbeat(self.device_id)
                
                # Check if it has GPS coordinates
                loc_data = parse_ascii_location(segments)
                if loc_data:
                    logger.info(
                        "[%s] ASCII GPS lat=%.6f lon=%.6f spd=%.1fkm/h dir=%d",
                        self.device_id,
                        loc_data["latitude"],
                        loc_data["longitude"],
                        loc_data["speed"],
                        loc_data["direction"],
                    )
                    # Persist to TimescaleDB
                    try:
                        session_factory = get_session_factory()
                        async with session_factory() as session:
                            track = GPSTrack(
                                time=datetime.now(timezone.utc),
                                device_id=self.device_id,
                                latitude=loc_data["latitude"],
                                longitude=loc_data["longitude"],
                                speed=loc_data["speed"],
                                direction=loc_data["direction"],
                                elevation=loc_data["elevation"],
                                alarm_flags=0,
                                status_flags=12,
                            )
                            session.add(track)
                            await session.commit()
                    except Exception as exc:
                        logger.error("[%s] DB write failed: %s", self.device_id, exc)
                else:
                    # Log why GPS was skipped (helps debug 0,0 or missing coords)
                    if len(segments) >= 6:
                        logger.debug(
                            "[%s] GPS not persisted (lat=%s lon=%s) – invalid or zero coords",
                            self.device_id,
                            segments[4] if len(segments) > 4 else "N/A",
                            segments[5] if len(segments) > 5 else "N/A",
                        )
            else:
                logger.debug("[%s] Packet has only %d segments, skipping", self.addr, len(segments))


        # Process first chunk
        decoded_chunk = first_chunk.decode('ascii', errors='ignore')
        ascii_buffer += decoded_chunk
        
        while '#' in ascii_buffer:
            parts = ascii_buffer.split('#', 1)
            packet = parts[0] + '#'
            ascii_buffer = parts[1]
            await process_ascii_packet(packet)

        # Read loop
        while True:
            data = await asyncio.wait_for(
                self.reader.read(4096),
                timeout=float(settings.DEVICE_TTL_SECONDS),
            )
            if not data:
                break
            
            decoded_chunk = data.decode('ascii', errors='ignore')
            ascii_buffer += decoded_chunk
            
            while '#' in ascii_buffer:
                parts = ascii_buffer.split('#', 1)
                packet = parts[0] + '#'
                ascii_buffer = parts[1]
                await process_ascii_packet(packet)

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
        self.b64_path = generate_dynamic_rtsp_path(self.device_id)
        active_connections[self.device_id] = self
        await store.register_device(self.device_id, self.addr)

        # Generate HMAC stream token and persist in Redis
        token = generate_stream_token(self.device_id, settings.SECRET_KEY)
        await store.store_stream_token(self.device_id, token)

        # Reply 0x8100: seq(WORD) + result(BYTE=0 success) + auth_code
        auth_code = self.device_id.encode("ascii")
        body = struct.pack(">H", pkt.msg_seq) + b'\x00' + auth_code
        self.writer.write(build_packet(0x8100, self.phone_bcd, pkt.msg_seq, body))
        await self.writer.drain()
        logger.info("[%s] Registered successfully (path=%s...)", self.device_id, self.b64_path[:16])

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
        # If proxy has taken over this socket, do NOT close it or deregister the device.
        if self.proxying:
            logger.info("[%s] Telemetry task exiting, proxy owns socket (device=%s)", self.addr, self.device_id)
            return

        logger.info("[%s] Connection closed (device=%s)", self.addr, self.device_id)
        if self.device_id:
            active_connections.pop(self.device_id, None)
            await store.remove_device(self.device_id)
        self.writer.close()
        try:
            await self.writer.wait_closed()
        except Exception:
            pass


# ── Pump helpers ─────────────────────────────────────────────────────────────

async def _pump_c_to_m(
    c_reader: asyncio.StreamReader,
    m_writer: asyncio.StreamWriter,
    device_id: str,
) -> None:
    """Pipe raw H.264 bytes from the camera socket up to MediaMTX."""
    try:
        while True:
            chunk = await c_reader.read(65536)
            if not chunk:
                break
            # Strip ASCII telemetry keep-alives that may interrupt the video stream
            if chunk.startswith(b"$$") and b"#" in chunk:
                end_idx = chunk.find(b"#")
                if end_idx != -1:
                    chunk = chunk[end_idx + 1:]
            if chunk:
                m_writer.write(chunk)
                await m_writer.drain()
    except Exception as exc:
        logger.debug("[%s] cam→mediamtx pump ended: %s", device_id, exc)
    finally:
        try:
            m_writer.close()
        except Exception:
            pass


async def _pump_m_to_c(
    m_reader: asyncio.StreamReader,
    c_writer: asyncio.StreamWriter,
    device_id: str,
) -> None:
    """Pipe RTSP commands from MediaMTX down to the camera socket."""
    try:
        while True:
            chunk = await m_reader.read(65536)
            if not chunk:
                break
            c_writer.write(chunk)
            await c_writer.drain()
    except Exception as exc:
        logger.debug("[%s] mediamtx→cam pump ended: %s", device_id, exc)


# ── Server entry-points ──────────────────────────────────────────────────────

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


async def start_proxy_server(host: str, port: int) -> None:
    """
    RTSP proxy server (port 6609).

    MediaMTX is configured with source=rtsp://app:6609/$path.
    When it wants a stream it opens a TCP connection here, sends an RTSP
    DESCRIBE with the Base64 path, and we splice that connection to the
    matching live camera socket.
    """
    async def _on_proxy_connect(
        m_reader: asyncio.StreamReader, m_writer: asyncio.StreamWriter
    ) -> None:
        peer = m_writer.get_extra_info("peername")
        logger.info("[PROXY] Incoming connection from MediaMTX %s", peer)

        try:
            # Read the initial RTSP request to extract the stream path
            header_bytes = await asyncio.wait_for(m_reader.read(4096), timeout=10.0)
            if not header_bytes:
                m_writer.close()
                return

            # Decode and look for the RTSP path in DESCRIBE / OPTIONS / PLAY line
            header_text = header_bytes.decode("utf-8", errors="ignore")
            requested_path: Optional[str] = None

            for line in header_text.splitlines():
                # Typical line: "DESCRIBE rtsp://host:port/<base64path> RTSP/1.0"
                if line.upper().startswith(("DESCRIBE", "OPTIONS", "SETUP", "PLAY", "ANNOUNCE")):
                    parts = line.split()
                    if len(parts) >= 2:
                        url = parts[1]
                        if url == "*":
                            continue
                        # Extract everything after the last '/' and strip query parameters
                        requested_path = url.rstrip("/").split("/")[-1].split("?")[0]
                        break

            if not requested_path:
                logger.warning("[PROXY] Could not extract RTSP path from request")
                m_writer.close()
                return

            # ── Resolve device_id from the path ──────────────────────────
            # MediaMTX sends whatever path the dashboard gave it. The
            # dashboard uses the HMAC stream token as the path:
            #   {signature},{device_id},{timestamp},{nonce}
            # Try HMAC decode first, fall back to b64_path matching.
            conn: Optional[DeviceConnection] = None

            # 1. Try HMAC token → device_id lookup
            device_id_from_token = verify_stream_token(
                requested_path, settings.SECRET_KEY
            )
            if device_id_from_token:
                conn = active_connections.get(device_id_from_token)
                logger.info(
                    "[PROXY] HMAC token resolved to device=%s", device_id_from_token
                )

            # 2. Fallback: match against b64_path (camera-direct RTSP push)
            if conn is None:
                for dev_conn in list(active_connections.values()):
                    if dev_conn.b64_path == requested_path:
                        conn = dev_conn
                        break

            if conn is None:
                logger.warning(
                    "[PROXY] No active camera for path=%s (token_device=%s)",
                    requested_path[:40],
                    device_id_from_token,
                )
                m_writer.close()
                return

            logger.info("[PROXY] Splicing MediaMTX to camera device=%s", conn.device_id)

            # Mark the camera socket as proxying and cancel its telemetry loop
            conn.proxying = True
            if conn.handle_task and not conn.handle_task.done():
                conn.handle_task.cancel()
                # Give the telemetry loop a moment to exit cleanly
                await asyncio.sleep(0.05)

            # Forward the initial request bytes to the camera
            conn.writer.write(header_bytes)
            await conn.writer.drain()

            # Bidirectional pipe: camera ↔ MediaMTX
            await asyncio.gather(
                _pump_c_to_m(conn.reader, m_writer, conn.device_id),
                _pump_m_to_c(m_reader, conn.writer, conn.device_id),
                return_exceptions=True,
            )

        except asyncio.TimeoutError:
            logger.warning("[PROXY] Timed out reading RTSP headers from MediaMTX")
        except Exception as exc:
            logger.error("[PROXY] Error: %s", exc, exc_info=True)
        finally:
            # After streaming ends, clean up the camera connection
            device_id = conn.device_id if conn else None
            if conn:
                conn.proxying = False
                active_connections.pop(device_id, None)
                store = get_device_store()
                try:
                    await store.remove_device(device_id)
                except Exception:
                    pass
                conn.writer.close()
            m_writer.close()
            logger.info("[PROXY] Stream ended for device=%s", device_id)

    server = await asyncio.start_server(_on_proxy_connect, host, port)
    addrs = ", ".join(str(s.getsockname()) for s in server.sockets)
    logger.info("RTSP Proxy Server listening on %s", addrs)

    async with server:
        await server.serve_forever()
