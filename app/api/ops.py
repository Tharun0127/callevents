import asyncio
import logging
import os
import time
from collections.abc import Iterator

from fastapi import APIRouter, Response
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    REGISTRY,
    CollectorRegistry,
    generate_latest,
    multiprocess,
)
from prometheus_client.core import GaugeMetricFamily
from prometheus_client.registry import Collector
from sqlalchemy import func, select, text

from app.broker import broker_ping, queue_depths
from app.db import async_session_factory
from app.models import Delivery
from app.redis_client import get_async_redis

router = APIRouter(tags=["ops"])
log = logging.getLogger(__name__)

_CHECK_TIMEOUT = 2.0


@router.get("/healthz", include_in_schema=False)
async def healthz() -> dict[str, str]:
    """Liveness: the process is up and serving. No dependency checks, by design."""
    return {"status": "ok"}


@router.get("/readyz", include_in_schema=False)
async def readyz() -> JSONResponse:
    async def check_db() -> None:
        async with async_session_factory()() as session:
            await session.execute(text("SELECT 1"))

    async def check_redis() -> None:
        await get_async_redis().ping()

    async def check_broker() -> None:
        await run_in_threadpool(broker_ping)

    checks = {"postgres": check_db(), "redis": check_redis(), "rabbitmq": check_broker()}
    results: dict[str, str] = {}
    for name, coro in checks.items():
        try:
            await asyncio.wait_for(coro, timeout=_CHECK_TIMEOUT + 1)
            results[name] = "ok"
        except Exception as exc:
            results[name] = f"error: {type(exc).__name__}"
    ok = all(v == "ok" for v in results.values())
    return JSONResponse(
        {"status": "ok" if ok else "unavailable", "checks": results}, status_code=200 if ok else 503
    )


class _BacklogCollector(Collector):
    """Samples queue depth and delivery status counts at scrape time, cached briefly."""

    def __init__(self, depths: dict[str, int], statuses: dict[str, int]) -> None:
        self._depths = depths
        self._statuses = statuses

    def collect(self) -> Iterator[GaugeMetricFamily]:
        q = GaugeMetricFamily("queue_depth", "Messages ready per broker queue", labels=["queue"])
        for name, n in self._depths.items():
            q.add_metric([name], n)
        yield q
        d = GaugeMetricFamily("deliveries_by_status", "Delivery rows per status", labels=["status"])
        for name, n in self._statuses.items():
            d.add_metric([name], n)
        yield d


_backlog_cache: tuple[float, dict[str, int], dict[str, int]] = (0.0, {}, {})


async def _sample_backlog() -> tuple[dict[str, int], dict[str, int]]:
    global _backlog_cache
    ts, depths, statuses = _backlog_cache
    if time.monotonic() - ts < 5:
        return depths, statuses
    try:
        depths = await run_in_threadpool(queue_depths)
    except Exception:
        log.warning("queue depth sample failed")
    try:
        async with async_session_factory()() as session:
            rows = await session.execute(
                select(Delivery.status, func.count()).group_by(Delivery.status)
            )
            statuses = {str(s.value): int(n) for s, n in rows.all()}
    except Exception:
        log.warning("delivery status sample failed")
    _backlog_cache = (time.monotonic(), depths, statuses)
    return depths, statuses


@router.get("/metrics", include_in_schema=False)
async def metrics() -> Response:
    if os.environ.get("PROMETHEUS_MULTIPROC_DIR"):
        # Aggregate the per process files written by every uvicorn worker.
        process_registry = CollectorRegistry()
        multiprocess.MultiProcessCollector(process_registry)
    else:
        process_registry = REGISTRY
    backlog_registry = CollectorRegistry()
    backlog_registry.register(_BacklogCollector(*await _sample_backlog()))
    return Response(
        generate_latest(process_registry) + generate_latest(backlog_registry),
        media_type=CONTENT_TYPE_LATEST,
    )
