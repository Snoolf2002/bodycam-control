# Bodycam Control Plane & Telemetry Gateway

A high-performance, self-hosted, open-source replacement for the CMSv6 camera management system. It authenticates, tracks, and streams live feeds from 4G SIM-enabled mobile bodycameras using a decoupled split-plane architecture.

---

## 🏗️ Architecture Overview

The system divides responsibilities into two distinct operations:

1. **Control Plane (Telemetry):** An asynchronous TCP socket gateway built on native Python `asyncio` that handles persistent telemetry links. It decodes standard **JT/T 808** protocols (registration, authentication, keep-alive heartbeats, and GPS reporting) and updates live state in Redis while persisting tracking data to TimescaleDB.
2. **Streaming Plane (Video/Audio):** Managed by **MediaMTX**, an open-source media proxy engine. Video feeds are ingested from cameras via RTSP on port `6604`. MediaMTX converts the stream dynamically to browser-friendly low-latency formats (WebRTC and LL-HLS) and secures the pipeline by querying our FastAPI webhook using cryptographically signed HMAC tokens.

```
                  ┌───────────────────┐
                  │ 4G SIM Bodycamera │
                  └─────────┬─────────┘
                            │
        ┌───────────────────┴───────────────────┐
        │ Control Plane                         │ Streaming Plane
        ▼ (TCP Port 6608)                       ▼ (RTSP Port 6604)
 ┌──────────────┐                       ┌──────────────┐
 │ Telemetry    │                       │   MediaMTX   │
 │ TCP Gateway  │                       │ Media Proxy  │
 └──────┬───────┘                       └──────┬───────┘
        │ Register & Heartbeats                │ Auth Webhook Query
        ▼                                      ▼
 ┌──────────────┐                       ┌──────────────┐
 │    Redis     │◄──────────────────────┤   FastAPI    │
 │ (Live State) │  Verify active state  │   Backend    │
 └──────────────┘                       └──────┬───────┘
                                               │ Persist coordinates
                                               ▼
                                        ┌──────────────┐
                                        │ TimescaleDB  │
                                        │ (GPS History)│
                                        └──────────────┘
```

---

## ⚡ Features

* **Stateful TCP Reassembly:** Built-in `PacketBuffer` prevents byte-stream fragmentation and coalescing issues inherent to raw TCP socket networks.
* **Redis Active Registry:** Eliminates local state so you can scale the gateway and webhook containers horizontally.
* **Cryptographic Token Verification:** Implements HMAC-SHA256 signature verification for RTSP paths to prevent stream hijacking or URL forging.
* **TimescaleDB Hypertable integration:** Relational GPS history stored as optimized time-series coordinates for efficient trail mapping and fast geo-queries.
* **Media Conversions:** Ingests raw interleaved H.264 + PCMA RTSP streams and remuxes them instantly to WebRTC (8889) and HLS (8888) out-of-the-box.

---

## 📁 Repository Structure

```
bodycam-control/
├── Dockerfile                  # Lean python:3.11-slim container config
├── docker-compose.yml          # Full-stack orchestrator
├── mediamtx.yml                # MediaMTX configuration
├── requirements.txt            # Python dependencies
├── .dockerignore               # Optimizes Docker build contexts
├── .env.example                # Template for server configuration
└── app/
    ├── main.py                 # Application entry point & service lifecycle
    ├── api/
    │   ├── dependencies.py     # Database and Redis connection pools
    │   └── routes.py           # Webhooks and REST endpoints
    ├── core/
    │   ├── config.py           # Environment-based configurations
    │   └── security.py         # HMAC-SHA256 generator/validator
    ├── gateway/
    │   ├── protocol_808.py     # Stateful packet reassembly & JT/T 808 parsing
    │   └── socket_server.py    # Asynchronous TCP client handler
    ├── models/
    │   └── database.py         # SQLAlchemy & TimescaleDB tables definition
    └── services/
        └── redis_store.py      # Active device mappings & token cache logic
```

---

## 🚀 Quick Start Deployment

Make sure you have **Docker** and **Docker Compose** installed on your server.

### 1. Clone and Prepare Environment
```bash
git clone https://github.com/Snoolf2002/bodycam-control.git
cd bodycam-control

cp .env.example .env
```

### 2. Configure Configuration Variables
Edit the `.env` file on your server:
```bash
nano .env
```
* **`SECRET_KEY`**: Set this to a long random hexadecimal string (used to sign stream tokens).
* **`POSTGRES_PASSWORD`**: Use a secure password for your database instance.
* **`DATABASE_URL`**: Update the password here as well: `postgresql+asyncpg://bodycam:YOUR_SECURE_PASSWORD@timescaledb:5432/bodycam`

### 3. Build & Run
Deploy the containers in background mode:
```bash
docker compose up --build -d
```

### 4. Open Firewall Ports
Your server must accept inbound connections on the following ports:

| Port | Protocol | Direction | Description |
|---|---|---|---|
| **`6608`** | TCP | Inbound | Control Plane (JT/T 808 Telemetry from bodycams) |
| **`6604`** | TCP | Inbound | Streaming Plane (RTSP stream ingest from bodycams) |
| **`8889`** | TCP/UDP| Inbound | WebRTC Playback (for browser views) |
| **`8888`** | TCP | Inbound | HLS Playback |
| **`8001`** | TCP | Inbound | Control API (Proxy through Nginx/Caddy with TLS) |

---

## 🛠️ API & Webhook Endpoints

* **`POST /webhook/rtsp_auth`**  
  Used by MediaMTX to authenticate streams. Validates time-bounded cryptographic HMAC-SHA256 tokens (`rtsp://<server>:6604/<HMAC_TOKEN_STRING>`) or legacy Base64 tokens. Returns a `200 OK` status only if the device is registered online.
* **`GET /devices`**  
  Returns a JSON array of all currently active/connected bodycameras.
* **`GET /devices/{device_id}/token`**  
  Generates a valid HMAC-SHA256 streaming token for an active device.
* **`GET /devices/{device_id}/location`**  
  Returns the latest geographic coordinates, speed, and status flags recorded for a given device.

---

## 📹 Configuring Physical Bodycameras

Configure the settings on your physical 4G bodycameras (typically done via administrative USB utility software or SMS command):

1. **Protocol Selection:** Set to standard satellite tracking/video transmission (CMSv6 / JT/T 808).
2. **Server IP:** Enter the public IP address or Domain Name of your server.
3. **Control / Target Port:** Set to `6608`.
4. **Streaming / Media Port:** Set to `6604`.
5. **Device ID:** Set to the camera's SIM or hardware identification string (e.g. `3000181`).
