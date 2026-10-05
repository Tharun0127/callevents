"""Local webhook receiver used by docker compose and the load test.

Returns 200 for most requests and fails a configurable share (FAIL_RATE) with a 500 so the
retry and dead letter paths are actually exercised. It also verifies X-Signature and counts
duplicate Idempotency-Keys, which is how the load test checks that no delivery was double sent.

FAIL_RATE        share of requests answered with 500, 0.0 to 1.0
LATENCY_MS       added latency per request
SIGNING_SECRET   endpoint secret used to verify X-Signature
"""

import asyncio
import hashlib
import hmac
import os
import random
import threading
from collections import Counter

from fastapi import FastAPI, Request, Response

FAIL_RATE = float(os.environ.get("FAIL_RATE", "0.1"))
LATENCY_MS = float(os.environ.get("LATENCY_MS", "0"))
SECRET = os.environ.get("SIGNING_SECRET", "whsec_demo_receiver_secret").encode()

app = FastAPI(title="test receiver")

_lock = threading.Lock()
_stats: Counter[str] = Counter()
_succeeded_keys: set[str] = set()


@app.post("/hooks/{name}")
async def hook(name: str, request: Request) -> Response:
    body = await request.body()
    if LATENCY_MS:
        await asyncio.sleep(LATENCY_MS / 1000)
    expected = hmac.new(SECRET, body, hashlib.sha256).hexdigest()
    sig_ok = hmac.compare_digest(expected, request.headers.get("x-signature", ""))
    key = request.headers.get("idempotency-key", "")
    with _lock:
        _stats["requests"] += 1
        _stats[f"requests:{name}"] += 1
        if not sig_ok:
            _stats["bad_signature"] += 1
            return Response(status_code=401)
        if random.random() < FAIL_RATE:
            _stats["injected_failures"] += 1
            return Response(status_code=500, content=b"injected failure")
        if key in _succeeded_keys:
            _stats["duplicate_successes"] += 1
        else:
            _succeeded_keys.add(key)
            _stats["unique_successes"] += 1
    return Response(status_code=200, content=b"ok")


@app.get("/stats")
async def stats() -> dict[str, int | float]:
    with _lock:
        return {**_stats, "fail_rate": FAIL_RATE}


@app.post("/reset")
async def reset() -> dict[str, str]:
    with _lock:
        _stats.clear()
        _succeeded_keys.clear()
    return {"status": "reset"}


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}
