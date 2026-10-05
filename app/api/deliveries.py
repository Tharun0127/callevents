import logging
import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.concurrency import run_in_threadpool
from sqlalchemy import DateTime, literal, select, text, tuple_, update
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth import CurrentTenant
from app.db import get_session
from app.models import DeadLetter, Delivery, DeliveryStatus
from app.pagination import InvalidCursor, decode_cursor, encode_cursor
from app.schemas import DeliveryOut, DeliveryPage, ReplayResponse

router = APIRouter(tags=["deliveries"])
log = logging.getLogger(__name__)

Session = Annotated[AsyncSession, Depends(get_session)]


@router.get("/v1/deliveries", response_model=DeliveryPage)
async def list_deliveries(
    auth: CurrentTenant,
    session: Session,
    status: DeliveryStatus | None = None,
    event_id: uuid.UUID | None = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 50,
    cursor: str | None = None,
) -> DeliveryPage:
    stmt = select(Delivery).where(Delivery.tenant_id == auth.tenant_id)
    if status:
        stmt = stmt.where(Delivery.status == status)
    if event_id:
        stmt = stmt.where(Delivery.event_id == event_id)
    if cursor:
        try:
            ts, last_id = decode_cursor(cursor)
        except InvalidCursor as exc:
            raise HTTPException(status_code=400, detail="invalid cursor") from exc
        stmt = stmt.where(
            tuple_(Delivery.created_at, Delivery.id)
            < tuple_(literal(ts, DateTime(timezone=True)), literal(last_id, PGUUID(as_uuid=True)))
        )
    rows = (
        await session.scalars(
            stmt.order_by(Delivery.created_at.desc(), Delivery.id.desc()).limit(limit + 1)
        )
    ).all()
    page = rows[:limit]
    next_cursor = (
        encode_cursor(page[-1].created_at, page[-1].id) if len(rows) > limit and page else None
    )
    return DeliveryPage(data=[DeliveryOut.model_validate(d) for d in page], next_cursor=next_cursor)


@router.get("/v1/deliveries/{delivery_id}", response_model=DeliveryOut)
async def get_delivery(
    delivery_id: uuid.UUID, auth: CurrentTenant, session: Session
) -> DeliveryOut:
    d = await session.scalar(
        select(Delivery).where(Delivery.id == delivery_id, Delivery.tenant_id == auth.tenant_id)
    )
    if d is None:
        raise HTTPException(status_code=404, detail="delivery not found")
    return DeliveryOut.model_validate(d)


def _enqueue_delivery(delivery_id: uuid.UUID) -> None:
    from app.tasks import deliver

    deliver.delay(str(delivery_id))


@router.post("/v1/deliveries/{delivery_id}/replay", status_code=202, response_model=ReplayResponse)
async def replay_delivery(
    delivery_id: uuid.UUID, auth: CurrentTenant, session: Session
) -> ReplayResponse:
    # Conditional UPDATE: only a dead delivery owned by this tenant can be replayed, and two
    # concurrent replay calls cannot both succeed.
    revived = await session.scalar(
        update(Delivery)
        .where(
            Delivery.id == delivery_id,
            Delivery.tenant_id == auth.tenant_id,
            Delivery.status == DeliveryStatus.DEAD,
        )
        .values(
            status=DeliveryStatus.PENDING,
            attempt_count=0,
            next_retry_at=text("now()"),
            claimed_at=None,
            last_error=None,
        )
        .returning(Delivery.id)
    )
    if revived is None:
        exists = await session.scalar(
            select(Delivery.status).where(
                Delivery.id == delivery_id, Delivery.tenant_id == auth.tenant_id
            )
        )
        if exists is None:
            raise HTTPException(status_code=404, detail="delivery not found")
        raise HTTPException(
            status_code=409, detail=f"only dead deliveries can be replayed (status={exists})"
        )
    await session.execute(
        update(DeadLetter)
        .where(DeadLetter.delivery_id == delivery_id, DeadLetter.replayed_at.is_(None))
        .values(replayed_at=text("now()"))
    )
    await session.commit()
    try:
        await run_in_threadpool(_enqueue_delivery, delivery_id)
    except Exception:
        # Row is pending; the dispatcher will pick it up once overdue.
        log.exception("replay enqueue failed", extra={"delivery_id": str(delivery_id)})
    log.info("delivery replayed", extra={"delivery_id": str(delivery_id)})
    return ReplayResponse(delivery_id=delivery_id, status=DeliveryStatus.PENDING)
