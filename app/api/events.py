import uuid
from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import DateTime, literal, select, tuple_
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth import CurrentTenant
from app.db import get_session
from app.models import Event
from app.pagination import InvalidCursor, decode_cursor, encode_cursor
from app.schemas import EventOut, EventPage

router = APIRouter(tags=["events"])

Session = Annotated[AsyncSession, Depends(get_session)]


@router.get("/v1/events", response_model=EventPage)
async def list_events(
    auth: CurrentTenant,
    session: Session,
    limit: Annotated[int, Query(ge=1, le=500)] = 50,
    cursor: str | None = None,
    event_type: str | None = None,
    call_id: str | None = None,
    received_after: datetime | None = None,
    received_before: datetime | None = None,
) -> EventPage:
    # Every query starts from the authenticated tenant; there is no code path without it.
    stmt = select(Event).where(Event.tenant_id == auth.tenant_id)
    if event_type:
        stmt = stmt.where(Event.event_type == event_type)
    if call_id:
        stmt = stmt.where(Event.call_id == call_id)
    if received_after:
        stmt = stmt.where(Event.received_at >= received_after)
    if received_before:
        stmt = stmt.where(Event.received_at < received_before)
    if cursor:
        try:
            ts, last_id = decode_cursor(cursor)
        except InvalidCursor as exc:
            raise HTTPException(status_code=400, detail="invalid cursor") from exc
        # Row value comparison matches the (tenant_id, received_at DESC, id DESC) index order.
        stmt = stmt.where(
            tuple_(Event.received_at, Event.id)
            < tuple_(literal(ts, DateTime(timezone=True)), literal(last_id, PGUUID(as_uuid=True)))
        )

    # Fetch one extra row to know whether another page exists without a COUNT.
    rows = (
        await session.scalars(
            stmt.order_by(Event.received_at.desc(), Event.id.desc()).limit(limit + 1)
        )
    ).all()
    page = rows[:limit]
    next_cursor = (
        encode_cursor(page[-1].received_at, page[-1].id) if len(rows) > limit and page else None
    )
    return EventPage(data=[EventOut.model_validate(e) for e in page], next_cursor=next_cursor)


@router.get("/v1/events/{event_id}", response_model=EventOut)
async def get_event(event_id: uuid.UUID, auth: CurrentTenant, session: Session) -> EventOut:
    event = await session.scalar(
        select(Event).where(Event.id == event_id, Event.tenant_id == auth.tenant_id)
    )
    if event is None:
        # 404, not 403, so a tenant cannot probe for the existence of other tenants' ids.
        raise HTTPException(status_code=404, detail="event not found")
    return EventOut.model_validate(event)
