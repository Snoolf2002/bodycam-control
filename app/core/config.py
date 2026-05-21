from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Central configuration loaded from environment variables or .env file."""

    # Redis
    REDIS_URL: str = "redis://redis:6379/0"
    DEVICE_TTL_SECONDS: int = 120

    # Database (TimescaleDB / PostgreSQL)
    DATABASE_URL: str = "postgresql+asyncpg://bodycam:bodycam@timescaledb:5432/bodycam"

    # TCP Telemetry Gateway
    GATEWAY_HOST: str = "0.0.0.0"
    GATEWAY_PORT: int = 6608
    PROXY_PORT: int = 6609

    # HMAC secret for stream token signing
    SECRET_KEY: str = "CHANGE-ME-IN-PRODUCTION"

    # Session Token for RTSP matching (loaded from environment)
    SESSION_TOKEN: str = ""

    # API
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8001

    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()
