# Bodycam Control Center

A production-grade, self-hosted control plane for JT/T 808 and V101 ASCII body-worn cameras. It provides real-time GPS tracking on an interactive map, live video streaming via HLS/WebRTC, and a browser-based management dashboard.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Component Reference](#component-reference)
3. [Port Map](#port-map)
4. [Data Flow Diagrams](#data-flow-diagrams)
5. [API Reference](#api-reference)
6. [Configuration](#configuration)
7. [Deployment](#deployment)
8. [Protocol Details](#protocol-details)
9. [Stream Authentication](#stream-authentication)
10. [Known Issues & Defects](#known-issues--defects)
11. [Troubleshooting](#troubleshooting)

---

## Architecture Overview

The system is composed of four Docker services that communicate over an internal bridge network.

```
┌─────────────────────────────────────────────────────────────────┐
│                         Docker Network                          │
│                                                                 │
│  ┌─────────────┐   TCP:6608   ┌──────────────────────────────┐ │
│  │  Bodycam    │─────────────▶│  app (FastAPI + TCP Gateway) │ │
│  │  Device     │              │  - REST API      :8001        │ │
│  │  (Camera)   │   RTSP push  │  - Telemetry GW  :6608        │ │
│  └─────────────┘──────────┐   └──────────┬───────────────────┘ │
│                           │              │ Webhook (auth)       │
│                           │   TCP:6604   ▼                      │
│                           │   ┌──────────────────────────────┐ │
│                           └──▶│  mediamtx                    │ │
│                               │  - RTSP ingest   :6604        │ │
│                               │  - HLS playback  :8888        │ │
│                               │  - WebRTC play   :8889        │ │
│                               └──────────────────────────────┘ │
│                                                                 │
│  ┌──────────────┐            ┌──────────────────────────────┐  │
│  │  redis:7     │            │  timescaledb (PostgreSQL 16)  │  │
│  │  :6379       │            │  :5432                        │  │
│  └──────────────┘            └──────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Component Reference

### `app` — FastAPI Backend & TCP Telemetry Gateway

**Source:** `app/` | **Image:** Custom (Python 3.11 slim)

The central brain of the system. Runs two concurrent async services in a single process:

#### 1. FastAPI HTTP Server (`app/main.py`, `app/api/routes.py`)
- Serves the REST API used by the browser dashboard.
- Provides an authentication webhook called by MediaMTX before accepting any stream publish or play request.
- Must run with **exactly 1 worker** (`workers=1`) because the TCP gateway runs as an asyncio task in-process.

#### 2. TCP Telemetry Gateway (`app/gateway/socket_server.py`)
- Listens on **TCP port 6608** for camera connections.
- Auto-detects protocol on first packet:
  - `$$...#` → **V101 ASCII** mode
  - `0x7E...0x7E` → **JT/T 808 Binary** mode
- Maintains an in-memory dictionary `active_connections: Dict[str, DeviceConnection]` mapping `device_id` to the live socket.
- On each incoming packet, updates the device heartbeat in Redis.
- Persists GPS coordinates to TimescaleDB.
- On each heartbeat, checks Redis for a pending command and dispatches it to the camera.

#### Key Classes

| Class | File | Purpose |
|---|---|---|
| `DeviceConnection` | `socket_server.py` | Manages one camera TCP socket. Handles protocol detection, dispatch, GPS persistence, and command sending. |
| `DeviceStore` | `services/redis_store.py` | Redis-backed state store for device registry, stream tokens, and command queue. |
| `PacketBuffer` | `gateway/protocol_808.py` | Stateful buffer that handles TCP fragmentation and coalescing for JT/T 808 binary frames. |
| `Settings` | `core/config.py` | Pydantic-settings config loaded from environment variables / `.env` file. |

---

### `mediamtx` — Media Server

**Image:** `bluenviron/mediamtx:latest-ffmpeg`

Handles all video stream ingestion and distribution. Cameras push an RTSP stream to port 6604 after receiving a `0x9101` command. MediaMTX then re-serves that stream as:
- **HLS** on port `8888` for broad browser compatibility.
- **WebRTC** on port `8889` for low-latency browser playback.

Every publish and play request is validated against the `app` webhook at `http://app:8001/webhook/rtsp_auth`.

---

### `redis` — Ephemeral Device State

**Image:** `redis:7-alpine`

Stores transient data that does not need to survive a restart. All keys use a `TTL` (default 120 seconds) so stale devices are automatically evicted.

| Key Pattern | Type | Purpose | TTL |
|---|---|---|---|
| `device:active:{device_id}` | String (JSON) | Device registration record (address, timestamps) | 120s |
| `stream:token:{device_id}` | String | The Base64 RTSP path used as the stream identifier | 3600s |
| `command:pending:{device_id}` | String (JSON) | Queued command awaiting the next device heartbeat | 300s |

---

### `timescaledb` — GPS Telemetry History

**Image:** `timescale/timescaledb:latest-pg16`

Stores all historical GPS location records. Uses a TimescaleDB hypertable on the `time` column for efficient time-series queries.

**Schema — `gps_tracks` table:**

| Column | Type | Notes |
|---|---|---|
| `id` | `BIGSERIAL` | Auto-increment |
| `time` | `TIMESTAMPTZ` | Hypertable partition key |
| `device_id` | `VARCHAR(20)` | Camera identifier |
| `latitude` | `DOUBLE PRECISION` | Decimal degrees |
| `longitude` | `DOUBLE PRECISION` | Decimal degrees |
| `speed` | `DOUBLE PRECISION` | km/h |
| `direction` | `INTEGER` | 0–359° |
| `elevation` | `INTEGER` | Meters |
| `alarm_flags` | `INTEGER` | JT/T 808 alarm bitmask |
| `status_flags` | `INTEGER` | JT/T 808 status bitmask |

---

## Port Map

| Port | Protocol | Service | Description |
|---|---|---|---|
| `8001` | TCP/HTTP | `app` | REST API & MediaMTX auth webhook |
| `6608` | TCP | `app` | Camera telemetry gateway (JT/T 808 / V101) |
| `6609` | TCP | `app` | *(Disabled)* Legacy RTSP proxy server |
| `6604` | TCP/RTSP | `mediamtx` | Camera RTSP stream ingestion |
| `8888` | TCP/HTTP | `mediamtx` | HLS playback (`/stream-path/index.m3u8`) |
| `8889` | TCP/HTTP | `mediamtx` | WebRTC playback |
| `9997` | TCP/HTTP | `mediamtx` | MediaMTX REST management API |
| `6379` | TCP | `redis` | Redis |
| `5435` | TCP | `timescaledb` | PostgreSQL (mapped from internal 5432) |

---

## Data Flow Diagrams

### Camera Registration Flow

```
Camera                  app:6608 (TCP Gateway)          Redis
  │                             │                          │
  │── TCP connect ─────────────▶│                          │
  │                             │                          │
  │── First packet ($$...# or   │                          │
  │   0x7E packet) ────────────▶│                          │
  │                             │── Protocol detected      │
  │                             │── register_device() ────▶│
  │                             │   store_stream_token()──▶│
  │◀── ACK (ASCII or 0x8100) ──│                          │
  │                             │── check pending cmds ───▶│
  │                             │                          │
```

### GPS Heartbeat Flow

```
Camera                  app:6608                     TimescaleDB
  │                         │                             │
  │── GPS packet ──────────▶│                             │
  │                         │── parse coordinates         │
  │                         │── heartbeat() → Redis TTL   │
  │                         │── INSERT gps_tracks ───────▶│
  │◀── ACK ────────────────│                             │
```

### Stream Start Flow (JT/T 1078 Push Model)

```
Browser Dashboard       app:8001              Camera (via :6608)     mediamtx:6604
      │                     │                         │                    │
      │─ POST /devices/{id}/│                         │                    │
      │  start-stream ─────▶│                         │                    │
      │                     │── send 0x9101 cmd ─────▶│                    │
      │                     │   (ip, port=6604,        │                    │
      │                     │    channel=1, ...)        │                    │
      │◀─ 200 OK ──────────│                         │                    │
      │                     │                         │── RTSP ANNOUNCE ──▶│
      │                     │                         │   (Base64 path)    │
      │                     │◀───────────── webhook: POST /webhook/rtsp_auth
      │                     │── 200 OK ──────────────────────────────────▶│
      │                     │                                              │
      │── GET :8888/{path}/ │                                              │
      │   index.m3u8 ──────────────────────────────────────────────────▶│
      │◀─ HLS segments ────────────────────────────────────────────────│
```

### RTSP Authentication Flow (MediaMTX → `app`)

```
mediamtx                          app:8001/webhook/rtsp_auth
    │                                          │
    │── POST (ip, user, path, action) ────────▶│
    │                                          │── Try Base64 decode of path
    │                                          │   path → SESSION_TOKEN,3,device_id,...
    │                                          │
    │                                          │   if action=publish → always OK
    │                                          │   if action=read → check Redis online
    │                                          │
    │◀── 200 {"status":"ok"} ─────────────────│
```

---

## API Reference

Base URL: `http://<SERVER_IP>:8001`

### `GET /devices`

Returns all currently registered devices.

**Response:**
```json
{
  "connected_devices": [
    {
      "device_id": "3000181",
      "address": "192.168.1.50:54321",
      "registered_at": 1716278400.0,
      "last_heartbeat": 1716278520.0
    }
  ],
  "count": 1
}
```

---

### `GET /devices/{device_id}/token`

Returns the Base64 RTSP path token for the given device.

**Response:**
```json
{
  "device_id": "3000181",
  "stream_token": "OEJGNkRFMjQ4..."
}
```

---

### `GET /devices/{device_id}/location`

Returns the most recent GPS record from TimescaleDB.

**Response:**
```json
{
  "device_id": "3000181",
  "latitude": 51.509865,
  "longitude": -0.118092,
  "speed": 12.5,
  "direction": 270,
  "elevation": 35,
  "time": "2024-05-21T07:40:00+00:00"
}
```

---

### `POST /devices/{device_id}/start-stream`

Dispatches the JT/T 1078 `0x9101` Real-time Audio/Video Transmission Request to the device. If the device is offline, the command is queued in Redis and will be delivered on the next heartbeat.

**Request Body:**
```json
{
  "ip": "YOUR_SERVER_PUBLIC_IP",
  "port": 6604,
  "channel": 1,
  "data_type": 0,
  "stream_type": 1
}
```

| Field | Description | Notes |
|---|---|---|
| `ip` | Server IP the camera should push to | Must be reachable by the camera |
| `port` | MediaMTX RTSP ingest port | Default: `6604` |
| `channel` | Logical camera channel | Use `1` for main camera (not `0`) |
| `data_type` | 0=Audio+Video, 1=Video only, 2=Audio only | |
| `stream_type` | 0=Main stream, 1=Sub stream | |

**Response (immediate send):**
```json
{
  "status": "ok",
  "message": "Start-stream ASCII command sent successfully",
  "device_id": "3000181"
}
```

**Response (queued):**
```json
{
  "status": "queued",
  "message": "Start-stream command queued, waiting for device to connect",
  "device_id": "3000181"
}
```

---

### `POST /webhook/rtsp_auth`

**Internal — called by MediaMTX only.** Validates RTSP publish and play requests.

Accepts two path formats:
1. **Base64 CMSv6** — the native format cameras use: `SESSION_TOKEN,3,device_id,0,1,0,0,0` encoded as Base64 (padding stripped).
2. **HMAC dot-token** — platform-issued signed tokens in format `{signature}.{device_id}.{timestamp}.{nonce}`.

---

### `GET /diagnose`

Diagnostic endpoint. Tests DNS resolution and TCP connectivity to all internal services.

---

## Configuration

Copy `.env.example` to `.env` and edit values before deploying.

```env
# HMAC secret for signing RTSP stream tokens (MUST change in production)
SECRET_KEY=CHANGE-ME-TO-A-RANDOM-64-CHAR-STRING

# Redis
REDIS_URL=redis://redis:6379/0
DEVICE_TTL_SECONDS=120          # How long a device stays "online" without a heartbeat

# TimescaleDB
DATABASE_URL=postgresql+asyncpg://bodycam:bodycam@timescaledb:5432/bodycam
POSTGRES_USER=bodycam
POSTGRES_PASSWORD=bodycam
POSTGRES_DB=bodycam

# TCP Telemetry Gateway
GATEWAY_HOST=0.0.0.0
GATEWAY_PORT=6608

# API
API_HOST=0.0.0.0
API_PORT=8001
```

---

## Deployment

### Prerequisites

- Docker and Docker Compose installed on the server.
- Port `6608`, `6604`, `8001`, `8888`, `8889` open on the server firewall.
- The server's **public IP** must be reachable by the cameras (required for the `0x9101` push command).

### Steps

```bash
# 1. Clone the repository
git clone <repository-url>
cd bodycam-control

# 2. Configure environment
cp .env.example .env
# Edit .env with your actual values, especially SECRET_KEY

# 3. Start all services
docker compose up -d --build

# 4. Check logs
docker compose logs -f app
docker compose logs -f mediamtx
```

### Updating on Server

```bash
git pull
docker compose down
docker compose up -d --build
```

---

## Protocol Details

### V101 ASCII Protocol

The camera sends ASCII text packets framed by `$$` (start) and `#` (end).

**Packet structure:**
```
$$<seq+check>,<length>,V101,<device_id>,<phone>,<datetime>,<gps_valid>,<lat>,<lon>,<speed>,<direction>,<elevation>#
```

**Example:**
```
$$dc0240,68,V101,3000181,,210524 071522,A0000,51.509865,-0.118092,0.0,0,35#
```

| Field | Index | Description |
|---|---|---|
| `seq+check` | 0 | Hex sequence + checksum (e.g. `dc0240`) |
| `length` | 1 | Total packet length |
| `protocol` | 2 | Always `V101` |
| `device_id` | 3 | Camera identifier |
| `phone` | 4 | Optional phone number (often empty) |
| `datetime` | 5 | `DDMMYY HHMMSS` |
| `gps_valid` | 6 | `A0000` = valid fix, `V0000` = no fix |
| `latitude` | 7 | Decimal degrees |
| `longitude` | 8 | Decimal degrees |
| `speed` | 9 | km/h |
| `direction` | 10 | 0–359° |
| `elevation` | 11 | Meters |

**ACK format sent by server:**
```
$$cd<cmd_type>,<length>,V101,<device_id>,<phone>,#
```

**Command format sent to camera (e.g. start stream):**
```
$$cd9101,<length>,V101,<device_id>,<phone>,<server_ip>,<port>,<channel>,<data_type>,<stream_type>#
```

---

### JT/T 808 Binary Protocol

Packets are framed by `0x7E` markers with `0x7D` escape sequences.

**Frame structure:**
```
0x7E | Header(12 bytes) | Body(N bytes) | XOR Checksum(1 byte) | 0x7E
```

**Header:**
| Offset | Size | Field |
|---|---|---|
| 0 | 2 | Message ID |
| 2 | 2 | Message Attributes (lower 10 bits = body length) |
| 4 | 6 | Phone number (BCD, 6 bytes) |
| 10 | 2 | Sequence number |

**Supported incoming message IDs:**

| ID | Name | Description |
|---|---|---|
| `0x0100` | Registration | Device first registers on the platform |
| `0x0102` | Authentication | Device re-authenticates after registration |
| `0x0002` | Heartbeat | Keep-alive pulse |
| `0x0200` | Location Report | GPS coordinates + speed + direction |

**Outgoing message IDs:**

| ID | Name | Description |
|---|---|---|
| `0x8100` | Registration Reply | Sent in response to `0x0100` |
| `0x8001` | Generic ACK | Sent in response to heartbeat, auth, etc. |
| `0x9101` | Start Stream (JT/T 1078) | Tells camera to push RTSP to server |

**`0x9101` body structure:**

| Offset | Size | Field | Notes |
|---|---|---|---|
| 0 | 1 | IP address length (N) | |
| 1 | N | IP address | ASCII string |
| 1+N | 2 | TCP port | Big-endian uint16 |
| 3+N | 2 | UDP port | Big-endian uint16 (0 for TCP mode) |
| 5+N | 1 | Channel number | Use `1` for main camera |
| 6+N | 1 | Data type | 0=AV, 1=V, 2=A |
| 7+N | 1 | Stream type | 0=Main, 1=Sub |

---

## Stream Authentication

The stream token is a **Base64-encoded CMSv6 path** that uniquely identifies each camera session. It is generated when the camera first registers on the telemetry gateway.

**Generation:**
```python
SESSION_TOKEN = "8BF6DE248647478581A01D6A42B2E452"  # Fixed session key
raw_payload = f"{SESSION_TOKEN},3,{device_id},0,1,0,0,0"
b64_path = base64.b64encode(raw_payload.encode()).decode().rstrip("=")
```

This path is stored in Redis as `stream:token:{device_id}` and is what the camera uses as the RTSP path when pushing to MediaMTX.

**How authentication works:**
1. Camera connects to `rtsp://<SERVER_IP>:6604/<b64_path>` and sends `ANNOUNCE`.
2. MediaMTX calls `POST http://app:8001/webhook/rtsp_auth` with `path=<b64_path>` and `action=publish`.
3. The webhook Base64-decodes the path, extracts `device_id`, and returns `{"status": "ok"}` to allow publishing.
4. When a browser requests HLS, the same webhook is called with `action=read`. The device must be online in Redis for the request to succeed.

---

## Known Issues & Defects

### ⚠️ Stream Error: "no stream is available on path"

**Symptom:** MediaMTX returns `{"status":"error","error":"no stream is available on path '...'}`.

**Root Cause:** The camera has not yet pushed the RTSP stream to MediaMTX. This happens because:

1. **`0x9101` command not received by camera** — The camera may have already closed its telemetry TCP socket when the command is sent. The gateway now skips the ACK when dispatching a stream command to keep the socket open longer.
2. **Camera firmware rejects channel 0** — The `channel` field in the start-stream request must be `1` (not `0`). Some firmware silently ignores commands with `channel: 0`.
3. **Server IP unreachable from camera** — The IP sent in the `0x9101` command must be the server's **public IP**, not `localhost` or `127.0.0.1`.
4. **Camera pushes raw JT/T 1078 RTP, not RTSP** — If the firmware sends raw RTP packets instead of a proper RTSP `ANNOUNCE`, MediaMTX will reject the connection. This would require a custom RTP demuxer or a different MediaMTX path configuration.

**Workaround steps:**
1. Confirm the camera received the `0x9101` command in `docker compose logs app`.
2. Look for MediaMTX logs showing an incoming connection on port 6604.
3. If no connection appears in MediaMTX logs, the camera is not pushing — the issue is in the command delivery or camera firmware.

---

### ⚠️ Device Shows as Active but No Socket Exists

**Symptom:** `GET /devices` returns a device, but sending a command returns 404.

**Root Cause:** The device entry in Redis persists for `DEVICE_TTL_SECONDS` (120s) after the connection drops, but the in-memory `active_connections` dict is cleared immediately. This is intentional design to avoid false "offline" flashing in the UI, but it means commands can return a `queued` response even if the camera is physically disconnected.

---

### ⚠️ ASCII Command Length Field

The V101 ASCII protocol requires that the `<length>` field in the packet header matches the total character length of the packet string. The `send_ascii_command` method finds this by iterating from 10 to 1000 and checking string length. If no exact match is found (rare edge case with payloads near boundary sizes), it falls back to `length=0` which some firmware may reject.

---

## Troubleshooting

### Check if cameras are connecting

```bash
docker compose logs -f app | grep "Device identified"
docker compose logs -f app | grep "Registered"
```

### Check if MediaMTX is receiving streams

```bash
docker compose logs -f mediamtx | grep "ANNOUNCE\|RTSP source\|timed out"
```

### Check MediaMTX path status via API

```bash
curl http://<SERVER_IP>:9997/v3/paths/list
```

### Manually test the auth webhook

```bash
curl -X POST http://<SERVER_IP>:8001/webhook/rtsp_auth \
  -H "Content-Type: application/json" \
  -d '{"ip":"","user":"","password":"","path":"<b64_path>","protocol":"rtsp","id":"","action":"publish","query":""}'
```

### Check device state in Redis

```bash
docker compose exec redis redis-cli keys "device:active:*"
docker compose exec redis redis-cli get "device:active:<device_id>"
docker compose exec redis redis-cli get "stream:token:<device_id>"
docker compose exec redis redis-cli get "command:pending:<device_id>"
```

### Run internal diagnostics

```bash
curl http://<SERVER_IP>:8001/diagnose
```
