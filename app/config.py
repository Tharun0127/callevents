from functools import lru_cache

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """All runtime configuration. Every field maps to an upper case env var."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_env: str = "development"
    log_level: str = "INFO"

    # Postgres. One URL, the driver suffix is swapped for async (asyncpg) and sync (psycopg).
    database_url: str = "postgresql://callevents:callevents@localhost:5432/callevents"
    db_pool_size: int = 10
    db_max_overflow: int = 10

    redis_url: str = "redis://localhost:6379/0"
    broker_url: str = "amqp://guest:guest@localhost:5672//"

    # Per provider inbound signing secrets.
    acmetel_signing_secret: str = "dev-acmetel-secret"
    voxly_signing_secret: str = "dev-voxly-secret"
    # Voxly signs a timestamp; reject anything older than this to stop replays of captured requests.
    voxly_signature_tolerance_seconds: int = 300

    # Delivery behaviour.
    delivery_timeout_seconds: float = 10.0
    delivery_max_attempts: int = 6
    delivery_backoff_base_seconds: float = 1.0
    delivery_backoff_factor: float = 4.0
    delivery_backoff_jitter: float = 0.2
    delivery_lease_seconds: int = 120
    delivery_dispatch_grace_seconds: int = 60
    # The dispatcher recovers deliveries whose message was lost. While the delivery queues hold
    # more than this many messages, an overdue row is far more likely to be queued than lost,
    # so the sweep is skipped instead of piling duplicate messages onto the backlog.
    delivery_dispatch_max_queue_depth: int = 1000

    # Outbound per endpoint token bucket defaults (overridable per endpoint).
    endpoint_rate_per_second: float = 50.0
    endpoint_burst: int = 100

    # Circuit breaker.
    circuit_failure_threshold: int = 5
    circuit_cooldown_seconds: int = 30

    # Read API per tenant sliding window.
    read_rate_limit: int = 600
    read_rate_window_seconds: int = 60

    endpoint_cache_ttl_seconds: int = 300
    api_key_cache_ttl_seconds: int = 300

    worker_metrics_port: int = 9100

    # Seed data, used by scripts/seed.py so the stack is usable right after compose up.
    # Production turns this off: the demo tenant's API key is public in this repo.
    seed_demo_data: bool = True
    demo_api_key: str = Field(default="ck_demo_0123456789abcdef0123456789abcdef")
    demo_endpoint_secret: str = "whsec_demo_receiver_secret"
    demo_receiver_base_url: str = "http://receiver:8080"

    @model_validator(mode="after")
    def _no_dev_secrets_in_production(self) -> "Settings":
        """Refuse to start in production with any secret still at its public development value."""
        if self.app_env.lower() != "production":
            return self
        defaults = type(self).model_fields
        checked = ["acmetel_signing_secret", "voxly_signing_secret"]
        if self.seed_demo_data:
            checked += ["demo_api_key", "demo_endpoint_secret"]
        unsafe = [name.upper() for name in checked if getattr(self, name) == defaults[name].default]
        if unsafe:
            raise ValueError(
                "APP_ENV=production but these are still development defaults: " + ", ".join(unsafe)
            )
        return self

    @property
    def async_database_url(self) -> str:
        return _with_driver(self.database_url, "asyncpg")

    @property
    def sync_database_url(self) -> str:
        return _with_driver(self.database_url, "psycopg")


def _with_driver(url: str, driver: str) -> str:
    scheme, rest = url.split("://", 1)
    base = scheme.split("+", 1)[0]
    if base == "postgres":
        base = "postgresql"
    return f"{base}+{driver}://{rest}"


@lru_cache
def get_settings() -> Settings:
    return Settings()
