import logging
import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.concurrency import run_in_threadpool
from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_session
from app.metrics import EVENTS_INGESTED, INGEST_REJECTED
from app.models import Event, ProviderAccount
from app.providers import SignatureError, get_provider
from app.schemas import IngestResponse

router = APIRouter(tags=["ingestion"])
log = logging.getLogger(__name__)

MAX_BODY_BYTES = 256 * 1024


def _enqueue_fanout(event_id: uuid.UUID) -> None:
    from app.tasks import fanout_event

    fanout_event.delay(str(event_id))


@router.post("/v1/events/{provider}", status_code=202, response_model=IngestResponse)
async def ingest(
    provider: str, request: Request, session: Annotated[AsyncSession, Depends(get_session)]
) -> IngestResponse:
    impl = get_provider(provider)
    if impl is None:
        raise HTTPException(status_code=404, detail=f"unknown provider '{provider}'")

    body = await request.body()
    if len(body) > MAX_BODY_BYTES:
        INGEST_REJECTED.labels(provider=provider, reason="too_large").inc()
        raise HTTPException(status_code=413, detail="payload too large")

    # Verify before parsing: unauthenticated bytes never reach the JSON parser or the database.
    try:
        impl.verify(request.headers, body)
    except SignatureError as exc:
        INGEST_REJECTED.labels(provider=provider, reason="signature").inc()
        log.warning("webhook signature rejected", extra={"provider": provider, "why": str(exc)})
        raise HTTPException(status_code=401, detail="invalid signature") from exc

    try:
        event = impl.parse(body)
    except ValidationError as exc:
        INGEST_REJECTED.labels(provider=provider, reason="validation").inc()
        raise HTTPException(
            status_code=422, detail=exc.errors(include_url=False, include_context=False)
        ) from exc

    tenant_id = await session.scalar(
        select(ProviderAccount.tenant_id).where(
            ProviderAccount.provider == provider,
            ProviderAccount.external_account_id == event.account_id,
        )
    )
    if tenant_id is None:
        INGEST_REJECTED.labels(provider=provider, reason="unknown_account").inc()
        raise HTTPException(status_code=404, detail="unknown provider account")

    stmt = (
        pg_insert(Event)
        .values(
            id=uuid.uuid4(),
            tenant_id=tenant_id,
            provider=provider,
            provider_event_id=event.provider_event_id,
            event_type=event.event_type,
            call_id=event.call_id,
            occurred_at=event.occurred_at,
            payload=event.payload,
        )
        .on_conflict_do_nothing(constraint="uq_events_tenant_provider_event")
        .returning(Event.id)
    )
    inserted_id = await session.scalar(stmt)
    duplicate = inserted_id is None
    if duplicate:
        event_id = await session.scalar(
            select(Event.id).where(
                Event.tenant_id == tenant_id,
                Event.provider_event_id == event.provider_event_id,
            )
        )
        assert event_id is not None
    else:
        assert inserted_id is not None
        event_id = inserted_id
    await session.commit()

    EVENTS_INGESTED.labels(provider=provider, duplicate=str(duplicate).lower()).inc()

    if not duplicate:
        # After commit, so the worker can always see the row. If the broker is unreachable the
        # event is still stored with fanned_out_at NULL and the beat sweeper enqueues it later.
        try:
            await run_in_threadpool(_enqueue_fanout, event_id)
        except Exception:
            log.exception(
                "fanout enqueue failed; sweeper will retry", extra={"event_id": str(event_id)}
            )

    return IngestResponse(event_id=event_id, duplicate=duplicate)
