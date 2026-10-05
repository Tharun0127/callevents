"""Opaque keyset cursors.

A cursor is base64url(JSON {"t": iso timestamp, "id": uuid}) of the last row on a page. The
next page is every row strictly older than that (timestamp, id) pair, so rows inserted while a
client is paging land before page one and never shift or duplicate later pages.
"""

import base64
import binascii
import json
import uuid
from datetime import datetime


class InvalidCursor(ValueError):
    pass


def encode_cursor(ts: datetime, row_id: uuid.UUID) -> str:
    raw = json.dumps({"t": ts.isoformat(), "id": str(row_id)}, separators=(",", ":"))
    return base64.urlsafe_b64encode(raw.encode()).decode().rstrip("=")


def decode_cursor(cursor: str) -> tuple[datetime, uuid.UUID]:
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        data = json.loads(base64.urlsafe_b64decode(padded.encode()))
        ts = datetime.fromisoformat(data["t"])
        if ts.tzinfo is None:
            raise InvalidCursor("cursor timestamp must be timezone aware")
        return ts, uuid.UUID(data["id"])
    except (binascii.Error, ValueError, KeyError, TypeError, json.JSONDecodeError) as exc:
        raise InvalidCursor("invalid cursor") from exc
