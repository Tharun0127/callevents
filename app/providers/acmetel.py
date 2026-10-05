"""AcmeTel: a flat, Twilio style payload.

Signature: X-Acme-Signature = hex(HMAC_SHA256(secret, raw_body)).
"""

from collections.abc import Mapping
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from app.providers.base import NormalizedEvent, Provider, SignatureError
from app.security import hmac_sha256_hex, signatures_match

AcmeStatus = Literal[
    "initiated", "ringing", "in-progress", "completed", "busy", "failed", "no-answer", "canceled"
]

_STATUS_MAP: dict[str, str] = {
    "initiated": "call.initiated",
    "ringing": "call.ringing",
    "in-progress": "call.answered",
    "completed": "call.completed",
    "busy": "call.failed",
    "failed": "call.failed",
    "no-answer": "call.failed",
    "canceled": "call.failed",
}


class AcmeTelWebhook(BaseModel):
    model_config = ConfigDict(extra="allow")

    EventSid: str = Field(min_length=1, max_length=128)
    AccountSid: str = Field(min_length=1, max_length=128)
    CallSid: str = Field(min_length=1, max_length=128)
    CallStatus: AcmeStatus
    From: str
    To: str
    Direction: Literal["inbound", "outbound"]
    Timestamp: datetime
    CallDuration: int | None = Field(default=None, ge=0)


class AcmeTel(Provider):
    name = "acmetel"
    header = "x-acme-signature"

    def __init__(self, secret: str) -> None:
        self._secret = secret

    def verify(self, headers: Mapping[str, str], body: bytes) -> None:
        provided = headers.get(self.header)
        if not provided:
            raise SignatureError("missing signature header")
        if not signatures_match(hmac_sha256_hex(self._secret, body), provided.strip()):
            raise SignatureError("signature mismatch")

    def parse(self, body: bytes) -> NormalizedEvent:
        hook = AcmeTelWebhook.model_validate_json(body)
        return NormalizedEvent(
            provider=self.name,
            provider_event_id=hook.EventSid,
            account_id=hook.AccountSid,
            event_type=_STATUS_MAP[hook.CallStatus],
            call_id=hook.CallSid,
            occurred_at=hook.Timestamp,
            payload={
                "from": hook.From,
                "to": hook.To,
                "direction": hook.Direction,
                "duration_seconds": hook.CallDuration,
                "provider_status": hook.CallStatus,
                "raw": hook.model_dump(mode="json"),
            },
        )
