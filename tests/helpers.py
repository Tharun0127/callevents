import uuid
from datetime import datetime

from sqlalchemy import func, select

from app.db import sync_session
from app.models import Delivery, DeliveryStatus, Event


def insert_event(
    tenant_id: uuid.UUID,
    *,
    event_type: str = "call.completed",
    call_id: str | None = None,
    received_at: datetime | None = None,
) -> uuid.UUID:
    event_id = uuid.uuid4()
    with sync_session() as s:
        e = Event(
            id=event_id,
            tenant_id=tenant_id,
            provider="acmetel",
            provider_event_id=f"EV{uuid.uuid4().hex}",
            event_type=event_type,
            call_id=call_id or f"CA{uuid.uuid4().hex[:8]}",
            payload={"from": "+1", "to": "+2"},
        )
        if received_at is not None:
            e.received_at = received_at
        s.add(e)
    return event_id


def insert_delivery(
    tenant_id: uuid.UUID,
    event_id: uuid.UUID,
    endpoint_id: uuid.UUID,
    status: DeliveryStatus = DeliveryStatus.PENDING,
    attempt_count: int = 0,
) -> uuid.UUID:
    delivery_id = uuid.uuid4()
    with sync_session() as s:
        s.add(
            Delivery(
                id=delivery_id,
                tenant_id=tenant_id,
                event_id=event_id,
                endpoint_id=endpoint_id,
                status=status,
                attempt_count=attempt_count,
            )
        )
    return delivery_id


def get_delivery(delivery_id: uuid.UUID) -> Delivery:
    with sync_session() as s:
        d = s.get(Delivery, delivery_id)
        assert d is not None
        s.expunge(d)
        return d


def count(model: type, *where: object) -> int:
    with sync_session() as s:
        return int(s.scalar(select(func.count()).select_from(model).where(*where)) or 0)
