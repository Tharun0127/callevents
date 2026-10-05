"""One delivery attempt, start to finish.

Ordering matters for the at most one in flight guarantee:

1. Read the row. Terminal or already in flight means another worker owns it: stop.
2. Ask the circuit breaker and the per endpoint token bucket. If either says wait, defer the
   task without consuming an attempt.
3. Claim with a conditional UPDATE (status pending/retrying -> in_flight, attempt_count + 1)
   and COMMIT before any network I/O. Only one worker can win this UPDATE, so a redelivered
   Celery message (acks_late after a crash) or a duplicate dispatcher enqueue finds nothing to
   claim and exits.
4. POST with a timeout. Record the outcome with an UPDATE guarded on status = in_flight.

A worker that dies between 3 and 4 leaves the row in_flight; the reaper returns it to
retrying after the lease expires. That case can resend a request the receiver already got,
which is why every request carries a stable Idempotency-Key (the delivery id). True exactly
once delivery to a third party over HTTP is not possible; this is at least once with
receiver side dedupe and no concurrent duplicates.
"""

import json
import logging
import random
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any

import httpx
from sqlalchemy import insert, select, text, update
from sqlalchemy.orm import Session

from app.cache import EndpointConfig, load_active_endpoints
from app.circuit import CircuitBreaker, Transition
from app.config import get_settings
from app.db import sync_session
from app.logs import request_id_var
from app.metrics import (
    CIRCUIT_TRANSITIONS,
    DEAD_LETTERS,
    DELIVERY_ATTEMPTS,
    DELIVERY_DEFERRED,
    DELIVERY_FAILURES,
    DELIVERY_LAG,
)
from app.models import CircuitEvent, DeadLetter, Delivery, DeliveryStatus, Event
from app.ratelimit import TokenBucket
from app.redis_client import get_redis
from app.security import hmac_sha256_hex

log = logging.getLogger(__name__)

RETRYABLE_STATUS = frozenset({408, 425, 429})
CLAIMABLE = (DeliveryStatus.PENDING, DeliveryStatus.RETRYING)
MAX_RETRY_AFTER_SECONDS = 300.0

_http_client: httpx.Client | None = None


def get_http_client() -> httpx.Client:
    global _http_client
    if _http_client is None:
        _http_client = httpx.Client(
            timeout=httpx.Timeout(get_settings().delivery_timeout_seconds),
            follow_redirects=False,
            limits=httpx.Limits(max_connections=200, max_keepalive_connections=50),
            headers={"User-Agent": "callevents-webhooks/0.1"},
        )
    return _http_client


def reset_http_client() -> None:
    global _http_client
    if _http_client is not None:
        _http_client.close()
    _http_client = None


def backoff_delay(attempt: int, rng: random.Random | None = None) -> float:
    """Delay before the next attempt after `attempt` (1 based) failed.

    base * factor ** (attempt - 1), i.e. 1, 4, 16, 64, 256 seconds with the defaults, then
    multiplied by a uniform jitter in [1 - j, 1 + j] so a burst of failures against one
    receiver does not retry in lockstep.
    """
    s = get_settings()
    nominal = s.delivery_backoff_base_seconds * s.delivery_backoff_factor ** (attempt - 1)
    r = rng or random
    return nominal * r.uniform(1 - s.delivery_backoff_jitter, 1 + s.delivery_backoff_jitter)


class OutcomeKind(StrEnum):
    SUCCEEDED = "succeeded"
    RETRY = "retry"
    DEAD = "dead"
    DEFERRED = "deferred"
    SKIPPED = "skipped"


@dataclass(frozen=True)
class Outcome:
    kind: OutcomeKind
    delay: float = 0.0
    detail: str = ""


def _breaker() -> CircuitBreaker:
    s = get_settings()
    return CircuitBreaker(get_redis(), s.circuit_failure_threshold, s.circuit_cooldown_seconds)


def build_body(event: Event) -> bytes:
    doc = {
        "id": str(event.id),
        "type": event.event_type,
        "call_id": event.call_id,
        "provider": event.provider,
        "occurred_at": event.occurred_at.isoformat() if event.occurred_at else None,
        "received_at": event.received_at.isoformat(),
        "data": event.payload,
    }
    return json.dumps(doc, separators=(",", ":"), sort_keys=True).encode()


def build_headers(
    secret: str, body: bytes, delivery_id: uuid.UUID, event_id: uuid.UUID, attempt: int
) -> dict[str, str]:
    headers = {
        "Content-Type": "application/json",
        "X-Signature": hmac_sha256_hex(secret, body),
        "X-Timestamp": str(int(time.time())),
        "Idempotency-Key": str(delivery_id),
        "X-Event-Id": str(event_id),
        "X-Delivery-Attempt": str(attempt),
    }
    rid = request_id_var.get()
    if rid:
        headers["X-Request-Id"] = rid
    return headers


def attempt_delivery(delivery_id: uuid.UUID) -> Outcome:
    s = get_settings()
    redis = get_redis()

    with sync_session() as session:
        row = session.execute(
            select(Delivery.status, Delivery.tenant_id, Delivery.endpoint_id).where(
                Delivery.id == delivery_id
            )
        ).one_or_none()
        if row is None:
            return Outcome(OutcomeKind.SKIPPED, detail="not_found")
        status, tenant_id, endpoint_id = row
        if status not in CLAIMABLE:
            return Outcome(OutcomeKind.SKIPPED, detail=str(status))
        endpoint = next(
            (
                e
                for e in load_active_endpoints(session, redis, tenant_id)
                if e.id == str(endpoint_id)
            ),
            None,
        )
        if endpoint is None:
            _dead_letter(session, delivery_id, "endpoint_inactive", from_status=CLAIMABLE)
            return Outcome(OutcomeKind.DEAD, detail="endpoint_inactive")

    breaker = _breaker()
    admission = breaker.admit(endpoint_id)
    if not admission.allowed:
        return _defer(delivery_id, admission.wait_seconds, "circuit_open")

    wait = TokenBucket(redis).take(endpoint_id, endpoint.rate_per_second, endpoint.burst)
    if wait > 0:
        return _defer(delivery_id, wait, "rate_limited")

    with sync_session() as session:
        attempt = session.execute(
            update(Delivery)
            .where(Delivery.id == delivery_id, Delivery.status.in_(CLAIMABLE))
            .values(
                status=DeliveryStatus.IN_FLIGHT,
                attempt_count=Delivery.attempt_count + 1,
                claimed_at=text("now()"),
            )
            .returning(Delivery.attempt_count)
        ).scalar_one_or_none()
        if attempt is None:
            return Outcome(OutcomeKind.SKIPPED, detail="claimed_elsewhere")
        event = session.execute(
            select(Event)
            .join(Delivery, Delivery.event_id == Event.id)
            .where(Delivery.id == delivery_id)
        ).scalar_one()
        body = build_body(event)
        event_id, received_at = event.id, event.received_at

    return _send(
        delivery_id,
        endpoint,
        breaker,
        attempt,
        body,
        event_id,
        received_at,
        s.delivery_max_attempts,
    )


def _send(
    delivery_id: uuid.UUID,
    endpoint: EndpointConfig,
    breaker: CircuitBreaker,
    attempt: int,
    body: bytes,
    event_id: uuid.UUID,
    received_at: datetime,
    max_attempts: int,
) -> Outcome:
    endpoint_id = uuid.UUID(endpoint.id)
    headers = build_headers(endpoint.secret, body, delivery_id, event_id, attempt)
    code: int | None = None
    retry_after: float | None = None
    started = time.perf_counter()
    try:
        resp = get_http_client().post(endpoint.url, content=body, headers=headers)
        code = resp.status_code
        if 200 <= code < 300:
            error = None
        else:
            error = f"HTTP {code}: {resp.text[:200]}"
            retry_after = _parse_retry_after(resp.headers.get("Retry-After"))
    except httpx.TimeoutException as exc:
        error = f"timeout: {type(exc).__name__}"
    except httpx.HTTPError as exc:
        error = f"transport: {type(exc).__name__}: {exc}"[:500]
    elapsed_ms = round((time.perf_counter() - started) * 1000, 1)

    log_ctx: dict[str, Any] = {
        "delivery_id": str(delivery_id),
        "endpoint_id": endpoint.id,
        "attempt": attempt,
        "status_code": code,
        "elapsed_ms": elapsed_ms,
    }

    if error is None:
        DELIVERY_ATTEMPTS.labels(outcome="success").inc()
        _record_transition(endpoint_id, breaker.record_success(endpoint_id), 0)
        with sync_session() as session:
            delivered_at = session.execute(
                update(Delivery)
                .where(Delivery.id == delivery_id, Delivery.status == DeliveryStatus.IN_FLIGHT)
                .values(
                    status=DeliveryStatus.SUCCEEDED,
                    delivered_at=text("now()"),
                    last_response_code=code,
                    last_error=None,
                    next_retry_at=None,
                )
                .returning(Delivery.delivered_at)
            ).scalar_one_or_none()
        if delivered_at is not None:
            DELIVERY_LAG.observe(max(0.0, (delivered_at - received_at).total_seconds()))
        log.info("delivery succeeded", extra=log_ctx)
        return Outcome(OutcomeKind.SUCCEEDED)

    DELIVERY_ATTEMPTS.labels(outcome="failure").inc()
    retryable = code is None or code >= 500 or code in RETRYABLE_STATUS
    reason = (
        "timeout"
        if error.startswith("timeout")
        else ("transport" if code is None else f"http_{code // 100}xx")
    )
    DELIVERY_FAILURES.labels(reason=reason).inc()
    log_ctx["error"] = error

    if retryable:
        fails, transition = breaker.record_failure(endpoint_id)
        _record_transition(endpoint_id, transition, fails)

    with sync_session() as session:
        if not retryable:
            _dead_letter(session, delivery_id, "non_retryable_status", code=code, error=error)
            log.warning("delivery failed permanently", extra=log_ctx)
            return Outcome(OutcomeKind.DEAD, detail="non_retryable_status")
        if attempt >= max_attempts:
            _dead_letter(session, delivery_id, "max_attempts", code=code, error=error)
            log.warning("delivery exhausted retries", extra=log_ctx)
            return Outcome(OutcomeKind.DEAD, detail="max_attempts")

        delay = backoff_delay(attempt)
        if retry_after is not None:
            delay = max(delay, min(retry_after, MAX_RETRY_AFTER_SECONDS))
        session.execute(
            update(Delivery)
            .where(Delivery.id == delivery_id, Delivery.status == DeliveryStatus.IN_FLIGHT)
            .values(
                status=DeliveryStatus.RETRYING,
                next_retry_at=datetime.now(UTC) + timedelta(seconds=delay),
                last_response_code=code,
                last_error=error,
            )
        )
    log.info("delivery failed, will retry", extra={**log_ctx, "retry_in_s": round(delay, 2)})
    return Outcome(OutcomeKind.RETRY, delay=delay, detail=reason)


MIN_DEFER_SECONDS = 0.25
DEFER_JITTER_SECONDS = 1.0


def _defer(delivery_id: uuid.UUID, wait: float, reason: str) -> Outcome:
    # The token bucket's wait is the time until one token exists, often a few milliseconds. With
    # thousands of deliveries queued for one endpoint, re-enqueueing all of them at that exact
    # moment makes them spin against the bucket. A floor plus jitter spreads them out.
    wait = max(wait, MIN_DEFER_SECONDS) + random.uniform(0, DEFER_JITTER_SECONDS)
    DELIVERY_DEFERRED.labels(reason=reason).inc()
    with sync_session() as session:
        session.execute(
            update(Delivery)
            .where(Delivery.id == delivery_id, Delivery.status.in_(CLAIMABLE))
            .values(next_retry_at=datetime.now(UTC) + timedelta(seconds=wait))
        )
    return Outcome(OutcomeKind.DEFERRED, delay=wait, detail=reason)


def _dead_letter(
    session: Session,
    delivery_id: uuid.UUID,
    reason: str,
    *,
    code: int | None = None,
    error: str | None = None,
    from_status: tuple[DeliveryStatus, ...] = (DeliveryStatus.IN_FLIGHT,),
) -> bool:
    row = session.execute(
        update(Delivery)
        .where(Delivery.id == delivery_id, Delivery.status.in_(from_status))
        .values(
            status=DeliveryStatus.DEAD,
            next_retry_at=None,
            last_response_code=code if code is not None else Delivery.last_response_code,
            last_error=error if error is not None else reason,
        )
        .returning(
            Delivery.tenant_id,
            Delivery.event_id,
            Delivery.endpoint_id,
            Delivery.attempt_count,
            Delivery.last_response_code,
            Delivery.last_error,
        )
    ).one_or_none()
    if row is None:
        return False
    session.execute(
        insert(DeadLetter).values(
            id=uuid.uuid4(),
            delivery_id=delivery_id,
            tenant_id=row.tenant_id,
            event_id=row.event_id,
            endpoint_id=row.endpoint_id,
            attempts=row.attempt_count,
            last_response_code=row.last_response_code,
            last_error=row.last_error,
            reason=reason,
        )
    )
    DEAD_LETTERS.labels(reason=reason).inc()
    log.error(
        "delivery dead lettered",
        extra={
            "delivery_id": str(delivery_id),
            "endpoint_id": str(row.endpoint_id),
            "attempts": row.attempt_count,
            "reason": reason,
            "last_response_code": row.last_response_code,
        },
    )
    return True


def dead_letter_by_id(delivery_id: uuid.UUID, reason: str) -> bool:
    """Backstop used by the task's on_failure when Celery itself gives up."""
    with sync_session() as session:
        return _dead_letter(
            session, delivery_id, reason, from_status=(*CLAIMABLE, DeliveryStatus.IN_FLIGHT)
        )


def _record_transition(endpoint_id: uuid.UUID, transition: Transition, fails: int) -> None:
    if transition is Transition.NONE:
        return
    CIRCUIT_TRANSITIONS.labels(state=transition.value).inc()
    log.warning(
        "circuit breaker transition",
        extra={"endpoint_id": str(endpoint_id), "state": transition.value, "failures": fails},
    )
    with sync_session() as session:
        session.add(
            CircuitEvent(
                endpoint_id=endpoint_id, state=transition.value, consecutive_failures=fails
            )
        )


def _parse_retry_after(value: str | None) -> float | None:
    if value is None:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        return None
