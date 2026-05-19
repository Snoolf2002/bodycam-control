import json
import time
from typing import Optional
from redis.asyncio import Redis


class DeviceStore:
    """Redis-backed ephemeral state store for active device connections."""

    def __init__(self, redis: Redis, ttl: int = 120):
        self.redis = redis
        self.ttl = ttl

    async def register_device(self, device_id: str, addr: tuple) -> None:
        data = {
            "address": f"{addr[0]}:{addr[1]}",
            "registered_at": time.time(),
            "last_heartbeat": time.time(),
        }
        await self.redis.set(
            f"device:active:{device_id}",
            json.dumps(data),
            ex=self.ttl,
        )

    async def heartbeat(self, device_id: str) -> None:
        raw = await self.redis.get(f"device:active:{device_id}")
        if raw:
            data = json.loads(raw)
            data["last_heartbeat"] = time.time()
            await self.redis.set(
                f"device:active:{device_id}",
                json.dumps(data),
                ex=self.ttl,
            )

    async def remove_device(self, device_id: str) -> None:
        await self.redis.delete(f"device:active:{device_id}")

    async def is_online(self, device_id: str) -> bool:
        return await self.redis.exists(f"device:active:{device_id}") > 0

    async def get_all_devices(self) -> list[dict]:
        devices: list[dict] = []
        async for key in self.redis.scan_iter("device:active:*"):
            key_str = key.decode() if isinstance(key, bytes) else key
            device_id = key_str.split(":")[-1]
            raw = await self.redis.get(key)
            if raw:
                data = json.loads(raw)
                data["device_id"] = device_id
                devices.append(data)
        return devices

    async def store_stream_token(self, device_id: str, token: str) -> None:
        await self.redis.set(f"stream:token:{device_id}", token, ex=3600)

    async def get_stream_token(self, device_id: str) -> Optional[str]:
        raw = await self.redis.get(f"stream:token:{device_id}")
        if raw is None:
            return None
        return raw.decode() if isinstance(raw, bytes) else raw
