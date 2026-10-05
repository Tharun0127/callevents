"""API key authentication and per tenant read rate limiting.

Keys look like ck_<key_id>_<secret>. key_id is stored in clear to find the tenant row; the full
key is stored as an Argon2id hash. Argon2 verification is deliberately slow (~tens of ms), so a
verified key is cached in Redis under sha256(key) for a few minutes. The cache never holds the
key itself, and deleting a tenant's key row plus `apikey:*` entries revokes it immediately.
"""

import uuid
from dataclasses import dataclass
from typing import Annotated

from fastapi import Depends, HTTPException, Request, Response
from fastapi.concurrency import run_in_threadpool
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.db import get_session
from app.metrics import RATE_LIMITED
from app.models import Tenant
from app.ratelimit import SlidingWindowLimiter
from app.redis_client import get_async_redis
from app.security import api_key_digest, parse_key_id, verify_api_key

_UNAUTHORIZED = HTTPException(
    status_code=401, detail="invalid or missing API key", headers={"WWW-Authenticate": "Bearer"}
)


@dataclass(frozen=True)
class AuthContext:
    tenant_id: uuid.UUID


def _extract_key(request: Request) -> str:
    header = request.headers.get("authorization", "")
    scheme, _, token = header.partition(" ")
    if scheme.lower() != "bearer" or not token.strip():
        raise _UNAUTHORIZED
    return token.strip()


async def authenticate(
    request: Request, session: Annotated[AsyncSession, Depends(get_session)]
) -> AuthContext:
    api_key = _extract_key(request)
    key_id = parse_key_id(api_key)
    if key_id is None:
        raise _UNAUTHORIZED

    redis = get_async_redis()
    cache_key = f"apikey:{api_key_digest(api_key)}"
    cached = await redis.get(cache_key)
    if cached:
        return AuthContext(tenant_id=uuid.UUID(cached))

    row = (
        await session.execute(
            select(Tenant.id, Tenant.api_key_hash).where(Tenant.api_key_id == key_id)
        )
    ).one_or_none()
    # Argon2 is CPU bound; keep it off the event loop.
    if row is None or not await run_in_threadpool(verify_api_key, api_key, row.api_key_hash):
        raise _UNAUTHORIZED
    await redis.set(cache_key, str(row.id), ex=get_settings().api_key_cache_ttl_seconds)
    return AuthContext(tenant_id=row.id)


async def rate_limited_tenant(
    response: Response, auth: Annotated[AuthContext, Depends(authenticate)]
) -> AuthContext:
    s = get_settings()
    limiter = SlidingWindowLimiter(get_async_redis(), s.read_rate_limit, s.read_rate_window_seconds)
    result = await limiter.hit(auth.tenant_id)
    headers = {
        "X-RateLimit-Limit": str(result.limit),
        "X-RateLimit-Remaining": str(result.remaining),
        "X-RateLimit-Reset": str(result.reset_seconds),
    }
    if not result.allowed:
        RATE_LIMITED.inc()
        headers["Retry-After"] = str(result.retry_after_seconds)
        raise HTTPException(status_code=429, detail="rate limit exceeded", headers=headers)
    response.headers.update(headers)
    return auth


CurrentTenant = Annotated[AuthContext, Depends(rate_limited_tenant)]
