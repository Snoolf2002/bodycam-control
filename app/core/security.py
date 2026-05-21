import hmac
import hashlib
import time
import secrets
from typing import Optional


def generate_stream_token(device_id: str, secret_key: str) -> str:
    """Create an HMAC-SHA256 signed token for RTSP stream authentication."""
    timestamp = str(int(time.time()))
    nonce = secrets.token_hex(8)
    message = f"{device_id}:{timestamp}:{nonce}"
    signature = hmac.new(
        secret_key.encode(), message.encode(), hashlib.sha256
    ).hexdigest()
    return f"{signature}.{device_id}.{timestamp}.{nonce}"


def verify_stream_token(
    token_string: str, secret_key: str, max_age: int = 3600
) -> Optional[str]:
    """
    Verify an HMAC-SHA256 signed stream token.
    Returns the device_id on success, None on failure.
    """
    try:
        parts = token_string.split(".")
        if len(parts) < 4:
            return None
        signature, device_id, timestamp, nonce = (
            parts[0], parts[1], parts[2], parts[3],
        )
        token_time = int(timestamp)
        if abs(time.time() - token_time) > max_age:
            return None
        message = f"{device_id}:{timestamp}:{nonce}"
        expected = hmac.new(
            secret_key.encode(), message.encode(), hashlib.sha256
        ).hexdigest()
        if hmac.compare_digest(signature, expected):
            return device_id
        return None
    except Exception:
        return None

