"""Per endpoint circuit breaker kept in Redis so every worker shares one view.

closed     deliveries flow; consecutive failures are counted.
open       after `threshold` consecutive failures; deliveries are deferred until the cooldown ends.
half_open  cooldown elapsed; exactly one probe delivery is let through (SET NX lock).
           Probe success closes the circuit, probe failure re-opens it for another cooldown.

State transitions are applied in Lua so two workers failing at once cannot both "open" it.
"""

import uuid
from dataclasses import dataclass
from enum import StrEnum

import redis

_ALLOW_LUA = """
local open = redis.call('GET', KEYS[2])
if not open then return {1, 0, 'closed'} end
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
if now < tonumber(open) then return {0, tonumber(open) - now, 'open'} end
if redis.call('SET', KEYS[3], '1', 'NX', 'PX', ARGV[1]) then return {1, 0, 'half_open'} end
return {0, 1000, 'half_open'}
"""

_FAILURE_LUA = """
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local fails = redis.call('INCR', KEYS[1])
local open = redis.call('GET', KEYS[2])
local threshold = tonumber(ARGV[1])
local cooldown = tonumber(ARGV[2])
if open then
  if now >= tonumber(open) then
    redis.call('SET', KEYS[2], now + cooldown)
    redis.call('DEL', KEYS[3])
    return {fails, 'reopened'}
  end
  return {fails, 'open'}
end
if fails >= threshold then
  redis.call('SET', KEYS[2], now + cooldown)
  return {fails, 'opened'}
end
return {fails, 'closed'}
"""

_SUCCESS_LUA = """
local open = redis.call('GET', KEYS[2])
redis.call('DEL', KEYS[1], KEYS[2], KEYS[3])
if open then return 'closed_from_open' end
return 'closed'
"""


class Transition(StrEnum):
    NONE = "none"
    OPENED = "opened"
    REOPENED = "reopened"
    CLOSED = "closed"


@dataclass(frozen=True)
class Admission:
    allowed: bool
    wait_seconds: float
    state: str


class CircuitBreaker:
    def __init__(self, client: redis.Redis, threshold: int, cooldown_seconds: int) -> None:
        self.threshold = threshold
        self.cooldown_ms = cooldown_seconds * 1000
        self._allow = client.register_script(_ALLOW_LUA)
        self._failure = client.register_script(_FAILURE_LUA)
        self._success = client.register_script(_SUCCESS_LUA)

    @staticmethod
    def _keys(endpoint_id: uuid.UUID) -> list[str]:
        base = f"cb:{endpoint_id}"
        return [f"{base}:fails", f"{base}:open_until", f"{base}:probe"]

    def admit(self, endpoint_id: uuid.UUID) -> Admission:
        allowed, wait_ms, state = self._allow(keys=self._keys(endpoint_id), args=[self.cooldown_ms])
        return Admission(bool(allowed), int(wait_ms) / 1000, str(state))

    def record_failure(self, endpoint_id: uuid.UUID) -> tuple[int, Transition]:
        fails, result = self._failure(
            keys=self._keys(endpoint_id), args=[self.threshold, self.cooldown_ms]
        )
        transition = {"opened": Transition.OPENED, "reopened": Transition.REOPENED}.get(
            str(result), Transition.NONE
        )
        return int(fails), transition

    def record_success(self, endpoint_id: uuid.UUID) -> Transition:
        result = self._success(keys=self._keys(endpoint_id))
        return Transition.CLOSED if result == "closed_from_open" else Transition.NONE
