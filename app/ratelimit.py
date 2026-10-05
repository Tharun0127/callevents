import secrets
import uuid
from dataclasses import dataclass

import redis
import redis.asyncio as aioredis

from app.redis_client import SLIDING_WINDOW_LUA, TOKEN_BUCKET_LUA


@dataclass(frozen=True)
class RateLimitResult:
    allowed: bool
    limit: int
    remaining: int
    retry_after_seconds: int
    reset_seconds: int


class SlidingWindowLimiter:
    """Per tenant limiter for the read API. Exact count over a rolling window."""

    def __init__(self, client: aioredis.Redis, limit: int, window_seconds: int) -> None:
        self.limit = limit
        self.window_ms = window_seconds * 1000
        self._script = client.register_script(SLIDING_WINDOW_LUA)

    async def hit(self, tenant_id: uuid.UUID) -> RateLimitResult:
        allowed, remaining, retry_ms, reset_ms = await self._script(
            keys=[f"rl:read:{tenant_id}"],
            args=[self.limit, self.window_ms, secrets.token_hex(6)],
        )
        return RateLimitResult(
            allowed=bool(allowed),
            limit=self.limit,
            remaining=int(remaining),
            retry_after_seconds=_ceil_seconds(int(retry_ms)),
            reset_seconds=_ceil_seconds(int(reset_ms)),
        )


class TokenBucket:
    """Per endpoint outbound limiter used by delivery workers."""

    def __init__(self, client: redis.Redis) -> None:
        self._script = client.register_script(TOKEN_BUCKET_LUA)

    def take(self, endpoint_id: uuid.UUID, rate_per_second: float, burst: int) -> float:
        """Return 0 when a token was taken, otherwise seconds to wait before trying again."""
        allowed, wait_ms = self._script(
            keys=[f"rl:endpoint:{endpoint_id}"], args=[rate_per_second, burst, 1]
        )
        return 0.0 if int(allowed) else int(wait_ms) / 1000


def _ceil_seconds(ms: int) -> int:
    return max(0, -(-ms // 1000))
