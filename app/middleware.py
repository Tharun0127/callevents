"""Pure ASGI middleware (cheaper than BaseHTTPMiddleware).

Assigns a request id, records the latency histogram and writes the access log.
"""

import logging
import re
import time

from starlette.types import ASGIApp, Message, Receive, Scope, Send

from app.logs import new_request_id, request_id_var
from app.metrics import HTTP_LATENCY

log = logging.getLogger("app.access")

_SAFE_ID = re.compile(r"^[A-Za-z0-9._-]{1,64}$")
_QUIET = {"/healthz", "/metrics"}


class RequestContextMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        incoming = dict(scope.get("headers") or []).get(b"x-request-id", b"").decode("latin-1")
        rid = incoming if _SAFE_ID.match(incoming) else new_request_id()
        token = request_id_var.set(rid)
        status = 500
        started = time.perf_counter()

        async def send_wrapper(message: Message) -> None:
            nonlocal status
            if message["type"] == "http.response.start":
                status = message["status"]
                message.setdefault("headers", [])
                message["headers"].append((b"x-request-id", rid.encode()))
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            elapsed = time.perf_counter() - started
            route = scope.get("route")
            path_template = getattr(route, "path", None) or "unmatched"
            HTTP_LATENCY.labels(scope["method"], path_template, str(status)).observe(elapsed)
            if path_template not in _QUIET:
                log.info(
                    "request",
                    extra={
                        "method": scope["method"],
                        "path": scope["path"],
                        "route": path_template,
                        "status": status,
                        "duration_ms": round(elapsed * 1000, 2),
                    },
                )
            request_id_var.reset(token)
