"""Endpoint configuration cache with explicit invalidation.

Plain "DEL on write" has a race: a reader loads old rows from Postgres, the writer commits and
deletes the key, then the reader writes the stale rows back and they live until the TTL. To
close that, each tenant has a version counter. Readers fetch the version first and cache under
`epcache:{tenant}:{version}`; writers INCR the version after commit. A slow reader can only
populate a key nobody will read again. The TTL is a backstop, not the invalidation mechanism.
"""

import json
import uuid
from dataclasses import asdict, dataclass

import redis
import redis.asyncio as aioredis
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.config import get_settings
from app.metrics import CACHE_REQUESTS
from app.models import Endpoint


@dataclass(frozen=True)
class EndpointConfig:
    id: str
    url: str
    secret: str
    event_types: list[str]
    rate_per_second: float
    burst: int

    def subscribes_to(self, event_type: str) -> bool:
        return "*" in self.event_types or event_type in self.event_types


def _ver_key(tenant_id: uuid.UUID) -> str:
    return f"epcache:ver:{tenant_id}"


def _data_key(tenant_id: uuid.UUID, version: str) -> str:
    return f"epcache:{tenant_id}:{version}"


def load_active_endpoints(
    session: Session, client: redis.Redis, tenant_id: uuid.UUID
) -> list[EndpointConfig]:
    version = client.get(_ver_key(tenant_id)) or "0"
    key = _data_key(tenant_id, str(version))
    cached = client.get(key)
    if cached is not None:
        CACHE_REQUESTS.labels(result="hit").inc()
        return [EndpointConfig(**row) for row in json.loads(str(cached))]

    CACHE_REQUESTS.labels(result="miss").inc()
    s = get_settings()
    rows = session.scalars(
        select(Endpoint).where(Endpoint.tenant_id == tenant_id, Endpoint.active.is_(True))
    ).all()
    configs = [
        EndpointConfig(
            id=str(e.id),
            url=e.url,
            secret=e.secret,
            event_types=list(e.event_types),
            rate_per_second=e.rate_per_second or s.endpoint_rate_per_second,
            burst=e.burst or s.endpoint_burst,
        )
        for e in rows
    ]
    client.set(key, json.dumps([asdict(c) for c in configs]), ex=s.endpoint_cache_ttl_seconds)
    return configs


async def invalidate_endpoints(client: aioredis.Redis, tenant_id: uuid.UUID) -> None:
    """Call after the write transaction commits."""
    await client.incr(_ver_key(tenant_id))
