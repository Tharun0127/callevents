import logging
import uuid
from datetime import UTC, datetime, timedelta

from sqlalchemy import select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.cache import load_active_endpoints
from app.config import get_settings
from app.db import sync_session
from app.models import Delivery, DeliveryStatus, Event
from app.redis_client import get_redis

log = logging.getLogger(__name__)


def fanout_event(event_id: uuid.UUID) -> list[uuid.UUID]:
    """Create one delivery per subscribed endpoint and return the ids that are new.

    Idempotent: the (event_id, endpoint_id) unique constraint plus ON CONFLICT DO NOTHING means
    a re-run inserts nothing and returns nothing, so it cannot enqueue a second send. If a
    previous run committed rows but died before enqueueing them, those rows are still pending
    and the dispatcher picks them up once they are overdue.
    """
    with sync_session() as session:
        event = session.get(Event, event_id)
        if event is None:
            log.warning("fanout for unknown event", extra={"event_id": str(event_id)})
            return []
        targets = [
            e
            for e in load_active_endpoints(session, get_redis(), event.tenant_id)
            if e.subscribes_to(event.event_type)
        ]
        new_ids: list[uuid.UUID] = []
        if targets:
            stmt = (
                pg_insert(Delivery)
                .values(
                    [
                        {
                            "id": uuid.uuid4(),
                            "tenant_id": event.tenant_id,
                            "event_id": event.id,
                            "endpoint_id": uuid.UUID(t.id),
                            "status": DeliveryStatus.PENDING,
                            "next_retry_at": datetime.now(UTC),
                        }
                        for t in targets
                    ]
                )
                .on_conflict_do_nothing(constraint="uq_deliveries_event_endpoint")
                .returning(Delivery.id)
            )
            new_ids = list(session.scalars(stmt).all())
        session.execute(
            update(Event)
            .where(Event.id == event_id, Event.fanned_out_at.is_(None))
            .values(fanned_out_at=text("now()"))
        )
    log.info(
        "fanout complete",
        extra={
            "event_id": str(event_id),
            "endpoints": len(targets),
            "new_deliveries": len(new_ids),
        },
    )
    return new_ids


def due_overdue_deliveries(limit: int = 500) -> list[uuid.UUID]:
    """Deliveries that should have run a while ago but have no live task (lost message).

    Rows are bumped forward so the next dispatcher tick does not pick them again while the
    freshly enqueued task is waiting in the queue.
    """
    grace = timedelta(seconds=get_settings().delivery_dispatch_grace_seconds)
    with sync_session() as session:
        ids = list(
            session.scalars(
                select(Delivery.id)
                .where(
                    Delivery.status.in_((DeliveryStatus.PENDING, DeliveryStatus.RETRYING)),
                    Delivery.next_retry_at < datetime.now(UTC) - grace,
                )
                .order_by(Delivery.next_retry_at)
                .limit(limit)
                .with_for_update(skip_locked=True)
            ).all()
        )
        if ids:
            session.execute(
                update(Delivery).where(Delivery.id.in_(ids)).values(next_retry_at=text("now()"))
            )
    return ids


def reap_stuck_in_flight(limit: int = 500) -> list[uuid.UUID]:
    """Return in_flight rows whose worker vanished (lease expired) to retrying."""
    lease = timedelta(seconds=get_settings().delivery_lease_seconds)
    with sync_session() as session:
        ids = list(
            session.scalars(
                select(Delivery.id)
                .where(
                    Delivery.status == DeliveryStatus.IN_FLIGHT,
                    Delivery.claimed_at < datetime.now(UTC) - lease,
                )
                .limit(limit)
                .with_for_update(skip_locked=True)
            ).all()
        )
        if ids:
            session.execute(
                update(Delivery)
                .where(Delivery.id.in_(ids), Delivery.status == DeliveryStatus.IN_FLIGHT)
                .values(
                    status=DeliveryStatus.RETRYING,
                    next_retry_at=text("now()"),
                    last_error="lease expired, worker presumed dead",
                )
            )
    if ids:
        log.warning("reaped stuck deliveries", extra={"count": len(ids)})
    return ids


def unfanned_events(older_than_seconds: int = 30, limit: int = 500) -> list[uuid.UUID]:
    """Events committed whose fanout enqueue was lost (e.g. broker down after commit)."""
    cutoff = datetime.now(UTC) - timedelta(seconds=older_than_seconds)
    with sync_session() as session:
        return list(
            session.scalars(
                select(Event.id)
                .where(Event.fanned_out_at.is_(None), Event.received_at < cutoff)
                .order_by(Event.received_at)
                .limit(limit)
            ).all()
        )
