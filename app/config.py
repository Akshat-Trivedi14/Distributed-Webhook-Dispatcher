"""Runtime configuration, loaded from environment variables (or a local .env file)."""
from __future__ import annotations

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", case_sensitive=False)

    # --- Storage -----------------------------------------------------------
    database_url: str = "postgresql://postgres:postgres@localhost:5432/webhooks"
    db_pool_size: int = 10

    # --- Process roles -----------------------------------------------------
    # The API and the worker can live in one process (default, ideal for a free
    # tier) or be split: RUN_WORKER=false for API-only, `python -m app.worker`
    # for worker-only. Any number of either can share the same Postgres.
    run_worker: bool = True
    enable_mock_receiver: bool = True

    # --- Delivery / retry policy ------------------------------------------
    # "Up to 3 retries" => 1 initial attempt + 3 retries = 4 attempts in total.
    max_retries: int = Field(3, ge=0, le=20)
    backoff_base_s: float = Field(2.0, gt=0)  # retry n waits base * 2^(n-1): 2s, 4s, 8s
    backoff_max_s: float = Field(300.0, gt=0)  # ceiling for backoff and Retry-After
    backoff_jitter: float = Field(0.0, ge=0, le=1)  # +0..x% random; 0 = exact schedule
    request_timeout_s: float = Field(10.0, gt=0)  # total deadline per delivery attempt

    # --- Worker -----------------------------------------------------------
    worker_concurrency: int = Field(20, ge=1)  # max in-flight deliveries per process
    poll_interval_s: float = Field(0.5, gt=0)
    lease_timeout_s: float = Field(60.0, gt=0)  # a claimed job not finished by then is reclaimed
    reaper_interval_s: float = Field(15.0, gt=0)

    # --- Per-destination-host protection (token bucket, shared via Postgres) --
    rate_limit_per_sec: float = Field(5.0, gt=0)  # sustained deliveries/sec per host
    rate_limit_burst: int = Field(5, ge=1)  # bucket size
    max_inflight_per_host: int = Field(3, ge=1)  # simultaneous requests per host, per process

    # --- Ingestion protection ---------------------------------------------
    ingest_rate_limit_per_min: int = Field(600, ge=1)  # per client IP, sliding window
    max_body_bytes: int = 256 * 1024
    api_key: str | None = None  # if set, required as X-API-Key on /api/*

    # --- Security ---------------------------------------------------------
    allow_private_destinations: bool = False  # SSRF guard; enable only for local dev/tests
    signing_secret: str | None = None  # if set, deliveries carry an HMAC-SHA256 signature

    # --- Staying awake on Render's free tier ------------------------------
    # Render fills in RENDER_EXTERNAL_URL itself, so on Render this is switched on automatically.
    render_external_url: str | None = None
    self_ping_interval_s: float = Field(300.0, gt=0)

    log_level: str = "INFO"

    @property
    def max_attempts(self) -> int:
        return self.max_retries + 1


@lru_cache
def get_settings() -> Settings:
    return Settings()
