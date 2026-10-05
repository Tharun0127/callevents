"""Test fixtures. Tests run against real Postgres and Redis (the compose services or CI service
containers), never SQLite or fakeredis, because the behaviour under test (ON CONFLICT, row value
comparison, Lua scripts, conditional UPDATE claims) is engine specific.

Celery runs in eager mode: .delay() executes inline in the calling thread, retries included.
"""

import os
import uuid
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass

# Configure before any app module reads settings.
os.environ["DATABASE_URL"] = os.environ.get(
    "TEST_DATABASE_URL", "postgresql://callevents:callevents@127.0.0.1:5432/callevents_test"
)
os.environ["REDIS_URL"] = os.environ.get("TEST_REDIS_URL", "redis://127.0.0.1:6379/15")
os.environ.setdefault("BROKER_URL", "amqp://callevents:callevents@127.0.0.1:5672//")
os.environ["LOG_LEVEL"] = "WARNING"

import httpx
import pytest
import respx
from sqlalchemy import create_engine, text

from alembic import command
from alembic.config import Config
from app.celery_app import celery_app
from app.config import get_settings
from app.db import dispose_async_engine, dispose_sync_engine, sync_session
from app.models import Endpoint, ProviderAccount, Tenant
from app.redis_client import get_redis
from app.security import generate_api_key, hash_api_key
from app.services.delivery import reset_http_client

TABLES = "circuit_events, dead_letters, deliveries, events, endpoints, provider_accounts, tenants"
RECEIVER = "http://receiver.test"


@pytest.fixture(scope="session", autouse=True)
def _database() -> Iterator[None]:
    url = get_settings().sync_database_url
    engine = create_engine(url)
    with engine.begin() as conn:
        conn.execute(text("DROP SCHEMA public CASCADE"))
        conn.execute(text("CREATE SCHEMA public"))
    engine.dispose()
    cfg = Config("alembic.ini")
    cfg.set_main_option("sqlalchemy.url", url)
    command.upgrade(cfg, "head")
    celery_app.conf.task_always_eager = True
    celery_app.conf.task_eager_propagates = True
    yield
    dispose_sync_engine()


@pytest.fixture(autouse=True)
def _clean_state() -> Iterator[None]:
    with sync_session() as s:
        s.execute(text(f"TRUNCATE {TABLES} CASCADE"))
    get_redis().flushdb()
    settings = get_settings()
    snapshot = settings.model_dump()
    yield
    for key, value in snapshot.items():
        setattr(settings, key, value)
    reset_http_client()


@pytest.fixture
async def api() -> AsyncIterator[httpx.AsyncClient]:
    from app.main import app

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://api.test") as client:
        yield client
    await dispose_async_engine()
    from app.redis_client import close_async_redis

    await close_async_redis()


@dataclass
class TenantFixture:
    id: uuid.UUID
    api_key: str
    acme_account: str
    voxly_account: str
    endpoint_ids: list[uuid.UUID]

    @property
    def headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.api_key}"}


def make_tenant(name: str, endpoints: list[tuple[str, list[str]]] | None = None) -> TenantFixture:
    key_id, api_key = generate_api_key()
    tenant_id = uuid.uuid4()
    acme, vox = f"AC_{name}", f"vx_{name}"
    endpoint_ids: list[uuid.UUID] = []
    with sync_session() as s:
        s.add(
            Tenant(id=tenant_id, name=name, api_key_id=key_id, api_key_hash=hash_api_key(api_key))
        )
        s.flush()
        s.add(ProviderAccount(tenant_id=tenant_id, provider="acmetel", external_account_id=acme))
        s.add(ProviderAccount(tenant_id=tenant_id, provider="voxly", external_account_id=vox))
        for path, types in endpoints or [("/hook", ["*"])]:
            eid = uuid.uuid4()
            s.add(
                Endpoint(
                    id=eid,
                    tenant_id=tenant_id,
                    url=f"{RECEIVER}{path}",
                    secret=f"whsec_{name}_secret",
                    event_types=types,
                )
            )
            endpoint_ids.append(eid)
    return TenantFixture(tenant_id, api_key, acme, vox, endpoint_ids)


@pytest.fixture
def tenant() -> TenantFixture:
    return make_tenant("alpha")


@pytest.fixture
def receiver() -> Iterator[respx.MockRouter]:
    """Intercepts outbound deliveries. Tests set the route's side effect."""
    with respx.mock(base_url=RECEIVER, assert_all_called=False) as router:
        yield router
