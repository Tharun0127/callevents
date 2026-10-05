"""Circuit breaker, replay, endpoint cache invalidation, outbound rate limit and ops endpoints."""

import time
import uuid

import httpx
import respx
from sqlalchemy import select

from app.cache import load_active_endpoints
from app.circuit import CircuitBreaker, Transition
from app.config import get_settings
from app.db import sync_session
from app.models import CircuitEvent, DeadLetter, DeliveryStatus
from app.ratelimit import TokenBucket
from app.redis_client import get_redis
from app.services.delivery import OutcomeKind, attempt_delivery
from tests.conftest import TenantFixture
from tests.helpers import get_delivery, insert_delivery, insert_event


def test_circuit_breaker_opens_half_opens_and_closes() -> None:
    breaker = CircuitBreaker(get_redis(), threshold=3, cooldown_seconds=1)
    ep = uuid.uuid4()

    assert breaker.admit(ep).allowed
    assert breaker.record_failure(ep) == (1, Transition.NONE)
    assert breaker.record_failure(ep) == (2, Transition.NONE)
    assert breaker.record_failure(ep) == (3, Transition.OPENED)

    blocked = breaker.admit(ep)
    assert not blocked.allowed and blocked.state == "open"
    assert 0 < blocked.wait_seconds <= 1

    time.sleep(1.1)
    probe = breaker.admit(ep)
    assert probe.allowed and probe.state == "half_open"
    # Only one probe at a time while half open.
    assert not breaker.admit(ep).allowed

    # Failed probe re-opens for another cooldown.
    assert breaker.record_failure(ep)[1] is Transition.REOPENED
    assert not breaker.admit(ep).allowed

    time.sleep(1.1)
    assert breaker.admit(ep).allowed
    assert breaker.record_success(ep) is Transition.CLOSED
    assert breaker.admit(ep).allowed and breaker.admit(ep).state == "closed"


def test_success_resets_consecutive_failure_count() -> None:
    breaker = CircuitBreaker(get_redis(), threshold=3, cooldown_seconds=30)
    ep = uuid.uuid4()
    breaker.record_failure(ep)
    breaker.record_failure(ep)
    assert breaker.record_success(ep) is Transition.NONE
    assert breaker.record_failure(ep) == (1, Transition.NONE)


def test_open_circuit_defers_deliveries_without_spending_attempts(
    tenant: TenantFixture, receiver: respx.MockRouter
) -> None:
    s = get_settings()
    s.circuit_failure_threshold = 2
    s.circuit_cooldown_seconds = 1
    route = receiver.post("/hook").respond(500)
    ids = [
        insert_delivery(tenant.id, insert_event(tenant.id), tenant.endpoint_ids[0])
        for _ in range(3)
    ]

    assert attempt_delivery(ids[0]).kind is OutcomeKind.RETRY
    assert attempt_delivery(ids[1]).kind is OutcomeKind.RETRY  # second failure opens it

    deferred = attempt_delivery(ids[2])
    assert deferred.kind is OutcomeKind.DEFERRED and deferred.detail == "circuit_open"
    assert route.call_count == 2
    assert get_delivery(ids[2]).attempt_count == 0
    assert get_delivery(ids[2]).status is DeliveryStatus.PENDING

    # After the cooldown a probe goes through; success closes the circuit.
    route.respond(200)
    time.sleep(1.1)
    assert attempt_delivery(ids[2]).kind is OutcomeKind.SUCCEEDED
    assert attempt_delivery(ids[0]).kind is OutcomeKind.SUCCEEDED

    with sync_session() as session:
        states = session.scalars(select(CircuitEvent.state).order_by(CircuitEvent.id)).all()
    assert states == ["opened", "closed"]


def test_outbound_token_bucket_limits_per_endpoint() -> None:
    bucket = TokenBucket(get_redis())
    ep, other = uuid.uuid4(), uuid.uuid4()
    waits = [bucket.take(ep, rate_per_second=1, burst=3) for _ in range(4)]
    assert waits[:3] == [0.0, 0.0, 0.0]
    assert 0 < waits[3] <= 1.0
    assert bucket.take(other, rate_per_second=1, burst=3) == 0.0


def test_rate_limited_endpoint_defers_delivery(
    tenant: TenantFixture, receiver: respx.MockRouter
) -> None:
    s = get_settings()
    s.endpoint_rate_per_second = 1
    s.endpoint_burst = 1
    receiver.post("/hook").respond(200)
    a = insert_delivery(tenant.id, insert_event(tenant.id), tenant.endpoint_ids[0])
    b = insert_delivery(tenant.id, insert_event(tenant.id), tenant.endpoint_ids[0])
    assert attempt_delivery(a).kind is OutcomeKind.SUCCEEDED
    second = attempt_delivery(b)
    assert second.kind is OutcomeKind.DEFERRED and second.detail == "rate_limited"
    assert get_delivery(b).attempt_count == 0


async def test_replay_requeues_a_dead_delivery(
    api: httpx.AsyncClient, tenant: TenantFixture, receiver: respx.MockRouter
) -> None:
    route = receiver.post("/hook").respond(500)
    get_settings().circuit_failure_threshold = 1000
    delivery_id = insert_delivery(tenant.id, insert_event(tenant.id), tenant.endpoint_ids[0])
    for _ in range(6):
        attempt_delivery(delivery_id)
    assert get_delivery(delivery_id).status is DeliveryStatus.DEAD

    dead = await api.get("/v1/deliveries", params={"status": "dead"}, headers=tenant.headers)
    assert [d["id"] for d in dead.json()["data"]] == [str(delivery_id)]

    route.respond(200)
    r = await api.post(f"/v1/deliveries/{delivery_id}/replay", headers=tenant.headers)
    assert r.status_code == 202

    # Eager Celery ran the delivery inline.
    d = get_delivery(delivery_id)
    assert d.status is DeliveryStatus.SUCCEEDED
    assert d.attempt_count == 1
    with sync_session() as session:
        dl = session.scalars(select(DeadLetter).where(DeadLetter.delivery_id == delivery_id)).one()
        assert dl.replayed_at is not None

    again = await api.post(f"/v1/deliveries/{delivery_id}/replay", headers=tenant.headers)
    assert again.status_code == 409


async def test_endpoint_writes_invalidate_cache_immediately(
    api: httpx.AsyncClient, tenant: TenantFixture
) -> None:
    redis = get_redis()
    with sync_session() as session:
        before = load_active_endpoints(session, redis, tenant.id)
    assert before[0].event_types == ["*"]

    ep = tenant.endpoint_ids[0]
    r = await api.patch(
        f"/v1/endpoints/{ep}", json={"event_types": ["call.failed"]}, headers=tenant.headers
    )
    assert r.status_code == 200
    with sync_session() as session:
        after = load_active_endpoints(session, redis, tenant.id)
    assert after[0].event_types == ["call.failed"]

    created = await api.post(
        "/v1/endpoints",
        json={"url": "https://example.com/hook", "event_types": ["call.completed"]},
        headers=tenant.headers,
    )
    assert created.status_code == 201
    assert created.json()["secret"].startswith("whsec_")
    with sync_session() as session:
        assert len(load_active_endpoints(session, redis, tenant.id)) == 2

    await api.delete(f"/v1/endpoints/{ep}", headers=tenant.headers)
    with sync_session() as session:
        assert len(load_active_endpoints(session, redis, tenant.id)) == 1


async def test_endpoint_validation(api: httpx.AsyncClient, tenant: TenantFixture) -> None:
    bad_type = await api.post(
        "/v1/endpoints",
        json={"url": "https://x.test", "event_types": ["sms.sent"]},
        headers=tenant.headers,
    )
    assert bad_type.status_code == 422
    bad_url = await api.post("/v1/endpoints", json={"url": "not a url"}, headers=tenant.headers)
    assert bad_url.status_code == 422
    listed = (await api.get("/v1/endpoints", headers=tenant.headers)).json()
    assert all("secret" not in e for e in listed)


async def test_health_ready_and_metrics(api: httpx.AsyncClient) -> None:
    assert (await api.get("/healthz")).json() == {"status": "ok"}
    ready = await api.get("/readyz")
    assert ready.status_code == 200, ready.text
    assert ready.json()["checks"] == {"postgres": "ok", "redis": "ok", "rabbitmq": "ok"}

    r = await api.get("/healthz", headers={"X-Request-ID": "trace-123"})
    assert r.headers["x-request-id"] == "trace-123"

    text = (await api.get("/metrics")).text
    for name in ("http_request_duration_seconds", "queue_depth", "delivery_attempts_total"):
        assert name in text


async def test_readyz_reports_unavailable_dependency(api: httpx.AsyncClient) -> None:
    s = get_settings()
    s.broker_url = "amqp://guest:guest@127.0.0.1:1//"
    r = await api.get("/readyz")
    assert r.status_code == 503
    assert r.json()["checks"]["rabbitmq"].startswith("error")
