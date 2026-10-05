import hashlib
import hmac
import json
import random
import threading
import time
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest
import respx
from celery import Task
from sqlalchemy import update

from app.config import get_settings
from app.db import sync_session
from app.models import DeadLetter, Delivery, DeliveryStatus
from app.services import delivery as svc
from app.services.delivery import OutcomeKind, attempt_delivery, backoff_delay
from app.services.fanout import fanout_event, reap_stuck_in_flight
from app.tasks import deliver
from tests.conftest import TenantFixture
from tests.helpers import count, get_delivery, insert_delivery, insert_event


@pytest.fixture(autouse=True)
def _no_breaker_interference() -> None:
    # Most tests here are about retries, not the breaker; keep it from deferring them.
    get_settings().circuit_failure_threshold = 1000


def _pending(tenant: TenantFixture) -> uuid.UUID:
    event_id = insert_event(tenant.id)
    return insert_delivery(tenant.id, event_id, tenant.endpoint_ids[0])


def test_backoff_schedule_is_base_four_with_bounded_jitter() -> None:
    rng = random.Random(42)
    for attempt, nominal in enumerate([1, 4, 16, 64, 256], start=1):
        samples = [backoff_delay(attempt, rng) for _ in range(500)]
        assert min(samples) >= nominal * 0.8
        assert max(samples) <= nominal * 1.2
        # Jitter actually spreads values instead of returning the nominal delay.
        assert max(samples) - min(samples) > nominal * 0.2


def test_each_failed_attempt_schedules_the_next_with_the_right_delay(
    tenant: TenantFixture, receiver: respx.MockRouter
) -> None:
    receiver.post("/hook").respond(503)
    delivery_id = _pending(tenant)

    for attempt, nominal in enumerate([1, 4, 16, 64, 256], start=1):
        before = datetime.now(UTC)
        outcome = attempt_delivery(delivery_id)
        assert outcome.kind is OutcomeKind.RETRY
        assert nominal * 0.8 <= outcome.delay <= nominal * 1.2
        d = get_delivery(delivery_id)
        assert d.status is DeliveryStatus.RETRYING
        assert d.attempt_count == attempt
        assert d.last_response_code == 503
        assert d.next_retry_at is not None
        scheduled = (d.next_retry_at - before).total_seconds()
        assert nominal * 0.8 - 1 <= scheduled <= nominal * 1.2 + 1


def test_exhausted_retries_land_in_dead_letter_table(
    tenant: TenantFixture, receiver: respx.MockRouter
) -> None:
    route = receiver.post("/hook").respond(500)
    delivery_id = _pending(tenant)

    outcomes = [attempt_delivery(delivery_id).kind for _ in range(6)]

    assert outcomes == [OutcomeKind.RETRY] * 5 + [OutcomeKind.DEAD]
    assert route.call_count == 6
    d = get_delivery(delivery_id)
    assert d.status is DeliveryStatus.DEAD
    assert d.attempt_count == 6
    with sync_session() as s:
        dl = s.query(DeadLetter).filter_by(delivery_id=delivery_id).one()
        assert dl.reason == "max_attempts"
        assert dl.attempts == 6
        assert dl.last_response_code == 500
    # A dead delivery is terminal: further task runs do nothing.
    assert attempt_delivery(delivery_id).kind is OutcomeKind.SKIPPED
    assert route.call_count == 6


def test_celery_autoretry_uses_our_countdowns_and_stops_at_six(
    tenant: TenantFixture, receiver: respx.MockRouter, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Runs through the real Celery task in eager mode, where retries execute inline."""
    route = receiver.post("/hook").mock(side_effect=httpx.ConnectTimeout("slow receiver"))
    countdowns: list[float] = []
    original = Task.retry

    def spy(self: Task, *args: Any, **kwargs: Any) -> Any:
        countdowns.append(kwargs["countdown"])
        return original(self, *args, **kwargs)

    monkeypatch.setattr(Task, "retry", spy)
    # With propagation on, eager mode raises Retry to the caller instead of re-applying.
    from app.celery_app import celery_app

    monkeypatch.setitem(celery_app.conf, "task_eager_propagates", False)
    delivery_id = _pending(tenant)

    deliver.delay(str(delivery_id))

    assert route.call_count == 6
    assert len(countdowns) == 5
    for c, nominal in zip(countdowns, [1, 4, 16, 64, 256], strict=True):
        assert nominal * 0.8 <= c <= nominal * 1.2
    assert get_delivery(delivery_id).status is DeliveryStatus.DEAD
    assert count(DeadLetter, DeadLetter.delivery_id == delivery_id) == 1


def test_non_retryable_4xx_dead_letters_immediately(
    tenant: TenantFixture, receiver: respx.MockRouter
) -> None:
    receiver.post("/hook").respond(410)
    delivery_id = _pending(tenant)
    assert attempt_delivery(delivery_id).kind is OutcomeKind.DEAD
    with sync_session() as s:
        assert s.query(DeadLetter).filter_by(delivery_id=delivery_id).one().reason == (
            "non_retryable_status"
        )


def test_429_retry_after_is_honoured(tenant: TenantFixture, receiver: respx.MockRouter) -> None:
    receiver.post("/hook").respond(429, headers={"Retry-After": "30"})
    outcome = attempt_delivery(_pending(tenant))
    assert outcome.kind is OutcomeKind.RETRY
    assert outcome.delay >= 30


def test_request_is_signed_and_carries_idempotency_key(
    tenant: TenantFixture, receiver: respx.MockRouter
) -> None:
    route = receiver.post("/hook").respond(200)
    delivery_id = _pending(tenant)

    assert attempt_delivery(delivery_id).kind is OutcomeKind.SUCCEEDED

    req = route.calls.last.request
    expected = hmac.new(b"whsec_alpha_secret", req.content, hashlib.sha256).hexdigest()
    assert req.headers["X-Signature"] == expected
    assert req.headers["Idempotency-Key"] == str(delivery_id)
    assert abs(int(req.headers["X-Timestamp"]) - time.time()) < 5
    assert json.loads(req.content)["type"] == "call.completed"
    d = get_delivery(delivery_id)
    assert d.status is DeliveryStatus.SUCCEEDED and d.delivered_at is not None


def test_concurrent_workers_send_exactly_once(
    tenant: TenantFixture, receiver: respx.MockRouter, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Simulates a redelivered message racing the original: only one may claim the row."""

    def slow(_: httpx.Request) -> httpx.Response:
        time.sleep(0.3)
        return httpx.Response(200)

    route = receiver.post("/hook").mock(side_effect=slow)
    delivery_id = _pending(tenant)
    results: list[OutcomeKind] = []
    # Hold every worker after the optimistic status read and before the claim, so all six
    # observe "pending" and only the conditional UPDATE can stop a double send.
    gate = threading.Barrier(6)
    real_load = svc.load_active_endpoints

    def gated_load(*args: Any) -> Any:
        found = real_load(*args)
        gate.wait()
        return found

    monkeypatch.setattr(svc, "load_active_endpoints", gated_load)

    def worker() -> None:
        results.append(attempt_delivery(delivery_id).kind)

    threads = [threading.Thread(target=worker) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert route.call_count == 1
    assert results.count(OutcomeKind.SUCCEEDED) == 1
    assert results.count(OutcomeKind.SKIPPED) == 5
    assert get_delivery(delivery_id).attempt_count == 1


def test_crashed_worker_lease_is_reaped_and_resent_with_same_key(
    tenant: TenantFixture, receiver: respx.MockRouter
) -> None:
    route = receiver.post("/hook").respond(200)
    delivery_id = _pending(tenant)
    # Simulate a worker that claimed the row and died before recording a result.
    with sync_session() as s:
        s.execute(
            update(Delivery)
            .where(Delivery.id == delivery_id)
            .values(
                status=DeliveryStatus.IN_FLIGHT,
                attempt_count=1,
                claimed_at=datetime.now(UTC) - timedelta(minutes=10),
            )
        )
    # While in flight nobody else may send it.
    assert attempt_delivery(delivery_id).kind is OutcomeKind.SKIPPED

    assert reap_stuck_in_flight() == [delivery_id]
    assert attempt_delivery(delivery_id).kind is OutcomeKind.SUCCEEDED
    assert route.calls.last.request.headers["Idempotency-Key"] == str(delivery_id)
    assert get_delivery(delivery_id).attempt_count == 2


def test_fanout_is_idempotent_and_respects_subscriptions(receiver: respx.MockRouter) -> None:
    from tests.conftest import make_tenant

    t = make_tenant(
        "subs",
        endpoints=[("/all", ["*"]), ("/done", ["call.completed"]), ("/fail", ["call.failed"])],
    )
    event_id = insert_event(t.id, event_type="call.completed")

    first = fanout_event(event_id)
    second = fanout_event(event_id)

    assert len(first) == 2
    assert second == []
    assert count(Delivery) == 2


def test_inactive_endpoint_is_not_targeted_and_pending_delivery_is_dead_lettered(
    tenant: TenantFixture, receiver: respx.MockRouter
) -> None:
    delivery_id = _pending(tenant)
    with sync_session() as s:
        from app.models import Endpoint

        s.execute(update(Endpoint).values(active=False))
    # Cached config must not hide the change in this test: invalidate like the API does.
    svc.get_redis().flushdb()
    assert attempt_delivery(delivery_id).kind is OutcomeKind.DEAD
    assert fanout_event(insert_event(tenant.id)) == []
