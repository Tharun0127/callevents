"""Builds correctly signed provider webhooks. Shared by the load test and the smoke script."""

import hashlib
import hmac
import json
import random
import time
import uuid
from datetime import UTC, datetime

ACME_STATUSES = ["initiated", "ringing", "in-progress", "completed", "failed"]
VOXLY_TYPES = ["call.started", "call.ringing", "call.answered", "call.ended", "call.failed"]


def _sign(secret: str, msg: bytes) -> str:
    return hmac.new(secret.encode(), msg, hashlib.sha256).hexdigest()


def acmetel(
    secret: str, account: str = "AC_demo", event_id: str | None = None
) -> tuple[bytes, dict[str, str]]:
    body = json.dumps(
        {
            "EventSid": event_id or f"EV{uuid.uuid4().hex}",
            "AccountSid": account,
            "CallSid": f"CA{uuid.uuid4().hex[:16]}",
            "CallStatus": random.choice(ACME_STATUSES),
            "From": "+14155550100",
            "To": "+14155550199",
            "Direction": "inbound",
            "Timestamp": datetime.now(UTC).isoformat(),
            "CallDuration": random.randint(0, 600),
        }
    ).encode()
    return body, {"Content-Type": "application/json", "X-Acme-Signature": _sign(secret, body)}


def voxly(
    secret: str, account: str = "vx_demo", event_id: str | None = None
) -> tuple[bytes, dict[str, str]]:
    body = json.dumps(
        {
            "id": event_id or f"evt_{uuid.uuid4().hex}",
            "type": random.choice(VOXLY_TYPES),
            "created_at": datetime.now(UTC).isoformat(),
            "account": {"id": account},
            "data": {
                "call": {
                    "uuid": str(uuid.uuid4()),
                    "direction": "outbound",
                    "from": {"number": "+442071838750"},
                    "to": {"number": "+442071838751"},
                    "duration_ms": random.randint(0, 600_000),
                }
            },
        }
    ).encode()
    ts = str(int(time.time()))
    sig = _sign(secret, ts.encode() + b"." + body)
    return body, {"Content-Type": "application/json", "Voxly-Signature": f"t={ts},v1={sig}"}
