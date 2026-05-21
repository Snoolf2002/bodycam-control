"""
JT/T 808 protocol parser with a stateful byte-buffer for safe TCP stream handling.

Handles:
  - 0x7E start/end markers
  - 0x7D escape sequences
  - XOR checksum validation
  - TCP fragmentation and coalescing via PacketBuffer
"""

import struct
import logging
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


# ── Low-level helpers ────────────────────────────────────────────────────────

def unescape(payload: bytes) -> bytes:
    result = bytearray()
    i = 0
    while i < len(payload):
        if payload[i] == 0x7D and i + 1 < len(payload):
            if payload[i + 1] == 0x02:
                result.append(0x7E)
                i += 2
                continue
            elif payload[i + 1] == 0x01:
                result.append(0x7D)
                i += 2
                continue
        result.append(payload[i])
        i += 1
    return bytes(result)


def escape(payload: bytes) -> bytes:
    result = bytearray()
    for b in payload:
        if b == 0x7D:
            result.extend(b'\x7d\x01')
        elif b == 0x7E:
            result.extend(b'\x7d\x02')
        else:
            result.append(b)
    return bytes(result)


def checksum(data: bytes) -> int:
    result = 0
    for b in data:
        result ^= b
    return result


# ── Packet builder ───────────────────────────────────────────────────────────

def build_packet(
    msg_id: int, phone: bytes, seq: int, body: bytes = b""
) -> bytes:
    """Build a complete 0x7E-framed JT/T 808 response packet."""
    header = struct.pack(">H", msg_id)
    header += struct.pack(">H", len(body))
    # Pad phone to 6 bytes
    if len(phone) < 6:
        phone = phone.rjust(6, b'\x00')
    header += phone[:6]
    header += struct.pack(">H", seq)
    payload = header + body
    chk = checksum(payload)
    escaped = escape(payload + bytes([chk]))
    return b'\x7e' + escaped + b'\x7e'


def build_9101_body(
    ip: str, tcp_port: int, udp_port: int, channel: int, data_type: int, stream_type: int
) -> bytes:
    """Build the body of 0x9101 Real-time Audio/Video Transmission Request."""
    ip_bytes = ip.encode("ascii")
    ip_len = len(ip_bytes)
    # Format:
    # B: IP address length (1 byte)
    # {ip_len}s: IP address (ip_len bytes)
    # H: Video server TCP port (2 bytes)
    # H: Video server UDP port (2 bytes)
    # B: Logical channel number (1 byte)
    # B: Data type (1 byte)
    # B: Stream type (1 byte)
    fmt = f">B{ip_len}sHHBBB"
    return struct.pack(fmt, ip_len, ip_bytes, tcp_port, udp_port, channel, data_type, stream_type)


# ── Parsed data structures ──────────────────────────────────────────────────

@dataclass
class ParsedPacket:
    msg_id: int
    phone_number: str
    msg_seq: int
    body: bytes


@dataclass
class LocationData:
    alarm_flags: int
    status_flags: int
    latitude: float
    longitude: float
    elevation: int
    speed: float        # km/h
    direction: int      # 0-359
    timestamp: str      # BCD YYMMDDHHMMSS


# ── Parsers ──────────────────────────────────────────────────────────────────

def parse_packet(raw_content: bytes) -> ParsedPacket:
    """
    Parse one packet's inner content (already stripped of outer 0x7E markers).
    """
    data = unescape(raw_content)
    body_and_header = data[:-1]
    pkt_checksum = data[-1]
    if checksum(body_and_header) != pkt_checksum:
        raise ValueError(
            f"Checksum mismatch: computed={checksum(body_and_header):#x} "
            f"got={pkt_checksum:#x}"
        )
    msg_id = struct.unpack(">H", body_and_header[0:2])[0]
    msg_attrs = struct.unpack(">H", body_and_header[2:4])[0]
    body_length = msg_attrs & 0x03FF
    phone_number = body_and_header[4:10].hex()
    msg_seq = struct.unpack(">H", body_and_header[10:12])[0]
    body = body_and_header[12:12 + body_length]
    return ParsedPacket(
        msg_id=msg_id,
        phone_number=phone_number,
        msg_seq=msg_seq,
        body=body,
    )


def parse_location(body: bytes) -> LocationData:
    """Parse 0x0200 Location Information Report body."""
    if len(body) < 28:
        raise ValueError(f"Location body too short: {len(body)} bytes")
    alarm_flags = struct.unpack(">I", body[0:4])[0]
    status_flags = struct.unpack(">I", body[4:8])[0]
    raw_lat = struct.unpack(">I", body[8:12])[0]
    raw_lon = struct.unpack(">I", body[12:16])[0]
    elevation = struct.unpack(">H", body[16:18])[0]
    raw_speed = struct.unpack(">H", body[18:20])[0]
    direction = struct.unpack(">H", body[20:22])[0]
    time_bcd = body[22:28].hex()

    latitude = raw_lat / 1_000_000.0
    longitude = raw_lon / 1_000_000.0
    speed = raw_speed / 10.0

    if status_flags & (1 << 2):
        latitude = -latitude
    if status_flags & (1 << 3):
        longitude = -longitude

    return LocationData(
        alarm_flags=alarm_flags,
        status_flags=status_flags,
        latitude=latitude,
        longitude=longitude,
        elevation=elevation,
        speed=speed,
        direction=direction,
        timestamp=time_bcd,
    )


# ── TCP stream buffer state-machine ─────────────────────────────────────────

class PacketBuffer:
    """
    Accumulates raw TCP bytes and yields complete JT/T 808 packets.

    Solves two classic TCP problems:
      1. Fragmentation – a single packet split across multiple read() calls.
      2. Coalescing   – multiple packets merged into a single read() call.
    """

    def __init__(self, max_size: int = 65536):
        self._buf = bytearray()
        self._max_size = max_size

    def feed(self, data: bytes) -> list[bytes]:
        """Feed raw bytes; returns list of raw inner payloads (no 0x7E)."""
        self._buf.extend(data)

        if len(self._buf) > self._max_size:
            logger.warning("Buffer overflow (%d bytes), resetting", len(self._buf))
            self._buf.clear()
            return []

        packets: list[bytes] = []
        while True:
            start = self._find_marker(0)
            if start == -1:
                self._buf.clear()
                break
            if start > 0:
                self._buf = self._buf[start:]

            end = self._find_marker(1)
            if end == -1:
                break

            raw_content = bytes(self._buf[1:end])
            self._buf = self._buf[end + 1:]
            if raw_content:
                packets.append(raw_content)

        return packets

    def _find_marker(self, start_offset: int) -> int:
        try:
            return self._buf.index(0x7E, start_offset)
        except ValueError:
            return -1
