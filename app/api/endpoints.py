import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Response
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth import CurrentTenant
from app.cache import invalidate_endpoints
from app.db import get_session
from app.models import Endpoint
from app.redis_client import get_async_redis
from app.schemas import EndpointCreate, EndpointCreated, EndpointOut, EndpointUpdate
from app.security import generate_endpoint_secret

router = APIRouter(tags=["endpoints"])

Session = Annotated[AsyncSession, Depends(get_session)]


async def _owned(session: AsyncSession, tenant_id: uuid.UUID, endpoint_id: uuid.UUID) -> Endpoint:
    ep = await session.scalar(
        select(Endpoint).where(Endpoint.id == endpoint_id, Endpoint.tenant_id == tenant_id)
    )
    if ep is None:
        raise HTTPException(status_code=404, detail="endpoint not found")
    return ep


@router.post("/v1/endpoints", status_code=201, response_model=EndpointCreated)
async def create_endpoint(
    body: EndpointCreate, auth: CurrentTenant, session: Session
) -> EndpointCreated:
    ep = Endpoint(
        id=uuid.uuid4(),
        tenant_id=auth.tenant_id,
        url=str(body.url),
        secret=body.secret or generate_endpoint_secret(),
        event_types=body.event_types,
        active=body.active,
        rate_per_second=body.rate_per_second,
        burst=body.burst,
    )
    session.add(ep)
    await session.commit()
    await session.refresh(ep)
    await invalidate_endpoints(get_async_redis(), auth.tenant_id)
    return EndpointCreated.model_validate(
        {**EndpointOut.model_validate(ep).model_dump(), "secret": ep.secret}
    )


@router.get("/v1/endpoints", response_model=list[EndpointOut])
async def list_endpoints(auth: CurrentTenant, session: Session) -> list[EndpointOut]:
    rows = (
        await session.scalars(
            select(Endpoint)
            .where(Endpoint.tenant_id == auth.tenant_id)
            .order_by(Endpoint.created_at)
        )
    ).all()
    return [EndpointOut.model_validate(e) for e in rows]


@router.get("/v1/endpoints/{endpoint_id}", response_model=EndpointOut)
async def get_endpoint(
    endpoint_id: uuid.UUID, auth: CurrentTenant, session: Session
) -> EndpointOut:
    return EndpointOut.model_validate(await _owned(session, auth.tenant_id, endpoint_id))


@router.patch("/v1/endpoints/{endpoint_id}", response_model=EndpointOut)
async def update_endpoint(
    endpoint_id: uuid.UUID, body: EndpointUpdate, auth: CurrentTenant, session: Session
) -> EndpointOut:
    ep = await _owned(session, auth.tenant_id, endpoint_id)
    changes = body.model_dump(exclude_unset=True)
    if "url" in changes and changes["url"] is not None:
        changes["url"] = str(changes["url"])
    for field, value in changes.items():
        if field in {"url", "event_types", "active"} and value is None:
            continue
        setattr(ep, field, value)
    await session.commit()
    await session.refresh(ep)
    await invalidate_endpoints(get_async_redis(), auth.tenant_id)
    return EndpointOut.model_validate(ep)


@router.delete("/v1/endpoints/{endpoint_id}", status_code=204)
async def delete_endpoint(
    endpoint_id: uuid.UUID, auth: CurrentTenant, session: Session
) -> Response:
    ep = await _owned(session, auth.tenant_id, endpoint_id)
    await session.delete(ep)
    await session.commit()
    await invalidate_endpoints(get_async_redis(), auth.tenant_id)
    return Response(status_code=204)
