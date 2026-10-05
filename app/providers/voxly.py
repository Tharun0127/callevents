"""Voxly: a nested envelope, Stripe style timestamped signature.

Signature: Voxly-Signature = "t=<unix seconds>,v1=<hex(HMAC_SHA256(secret, f"{t}.{raw_body}"))>".
Requests older than the tolerance are rejected even with a valid MAC, which stops replays of a
captured request outside the window. Inside the window the ingest uniqueness constraint makes a
replay a no op anyway.
"""

import time
from collections.abc import Mapping
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from app.providers.base import NormalizedEvent, Provider, SignatureError
from app.security import hmac_sha256_hex, signatures_match

VoxlyType = Literal["call.started", "call.ringing", "call.answered", "call.ended", "call.failed"]

_TYPE_MAP: dict[str, str] = {
    "call.started": "call.initiated",
    "call.ringing": "call.ringing",
    "call.answered": "call.answered",
    "call.ended": "call.completed",
    "call.failed": "call.failed",
}


class _Party(BaseModel):
    number: str


class _Call(BaseModel):
    model_config = ConfigDict(extra="allow")

    uuid: str = Field(min_length=1, max_length=128)
    direction: Literal["inbound", "outbound"]
    from_: _Party = Field(alias="from")
    to: _Party
    duration_ms: int | None = Field(default=None, ge=0)
    hangup_cause: str | None = None


class _Data(BaseModel):
    call: _Call


class _Account(BaseModel):
    id: str = Field(min_length=1, max_length=128)


class VoxlyWebhook(BaseModel):
    model_config = ConfigDict(extra="allow")

    id: str = Field(min_length=1, max_length=128)
    type: VoxlyType
    created_at: datetime
    account: _Account
    data: _Data


class Voxly(Provider):
    name = "voxly"
    header = "voxly-signature"

    def __init__(self, secret: str, tolerance_seconds: int = 300) -> None:
        self._secret = secret
        self._tolerance = tolerance_seconds

    def verify(self, headers: Mapping[str, str], body: bytes) -> None:
        raw = headers.get(self.header)
        if not raw:
            raise SignatureError("missing signature header")
        parts = dict(p.split("=", 1) for p in raw.split(",") if "=" in p)
        ts, provided = parts.get("t"), parts.get("v1")
        if not ts or not provided or not ts.isdigit():
            raise SignatureError("malformed signature header")
        if abs(time.time() - int(ts)) > self._tolerance:
            raise SignatureError("signature timestamp outside tolerance")
        expected = hmac_sha256_hex(self._secret, ts.encode() + b"." + body)
        if not signatures_match(expected, provided):
            raise SignatureError("signature mismatch")

    def parse(self, body: bytes) -> NormalizedEvent:
        hook = VoxlyWebhook.model_validate_json(body)
        call = hook.data.call
        return NormalizedEvent(
            provider=self.name,
            provider_event_id=hook.id,
            account_id=hook.account.id,
            event_type=_TYPE_MAP[hook.type],
            call_id=call.uuid,
            occurred_at=hook.created_at,
            payload={
                "from": call.from_.number,
                "to": call.to.number,
                "direction": call.direction,
                "duration_seconds": None if call.duration_ms is None else call.duration_ms // 1000,
                "provider_status": hook.type,
                "hangup_cause": call.hangup_cause,
                "raw": hook.model_dump(mode="json", by_alias=True),
            },
        )
