"""Redis clients and the Lua scripts used for atomic rate limiting.

Both a sync client (Celery workers) and an asyncio client (API) are exposed; the Lua source
is shared so the semantics are identical on both sides.
"""

import redis
import redis.asyncio as aioredis

from app.config import get_settings

_sync: redis.Redis | None = None
_async: aioredis.Redis | None = None


def get_redis() -> redis.Redis:
    global _sync
    if _sync is None:
        _sync = redis.Redis.from_url(
            get_settings().redis_url, decode_responses=True, health_check_interval=30
        )
    return _sync


def get_async_redis() -> aioredis.Redis:
    global _async
    if _async is None:
        _async = aioredis.Redis.from_url(
            get_settings().redis_url, decode_responses=True, health_check_interval=30
        )
    return _async


async def close_async_redis() -> None:
    global _async
    if _async is not None:
        await _async.aclose()
    _async = None


def reset_clients() -> None:
    """Used by tests when the Redis URL changes."""
    global _sync, _async
    _sync = None
    _async = None


# Sliding window log. KEYS[1] = zset key. ARGV = limit, window_ms, member_suffix.
# Uses the Redis server clock so every API replica agrees on "now".
# Returns {allowed (0/1), remaining, retry_after_ms, reset_ms}.
SLIDING_WINDOW_LUA = """
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
redis.call('ZREMRANGEBYSCORE', key, '-inf', now - window)
local count = redis.call('ZCARD', key)
if count < limit then
  redis.call('ZADD', key, now, now .. '-' .. ARGV[3])
  redis.call('PEXPIRE', key, window)
  local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
  local reset = tonumber(oldest[2]) + window - now
  return {1, limit - count - 1, 0, reset}
end
local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
local retry = tonumber(oldest[2]) + window - now
if retry < 1 then retry = 1 end
return {0, 0, retry, retry}
"""

# Token bucket. KEYS[1] = hash key. ARGV = rate_per_sec, capacity, requested.
# Returns {allowed (0/1), wait_ms}. Tokens refill continuously based on elapsed server time.
TOKEN_BUCKET_LUA = """
local key = KEYS[1]
local rate = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])
local requested = tonumber(ARGV[3])
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local state = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(state[1])
local ts = tonumber(state[2])
if tokens == nil then
  tokens = capacity
  ts = now
end
local elapsed = math.max(0, now - ts)
tokens = math.min(capacity, tokens + elapsed * rate / 1000)
local allowed = 0
local wait = 0
if tokens >= requested then
  tokens = tokens - requested
  allowed = 1
else
  wait = math.ceil((requested - tokens) * 1000 / rate)
end
redis.call('HSET', key, 'tokens', tokens, 'ts', now)
redis.call('PEXPIRE', key, math.ceil(capacity * 1000 / rate) + 1000)
return {allowed, wait}
"""
