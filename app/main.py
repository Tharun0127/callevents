import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api import deliveries, endpoints, events, ingest, ops
from app.config import get_settings
from app.db import dispose_async_engine, get_async_engine
from app.logs import configure_logging
from app.middleware import RequestContextMiddleware
from app.redis_client import close_async_redis

log = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    configure_logging(get_settings().log_level)
    get_async_engine()
    log.info("api starting")
    yield
    # Graceful shutdown: uvicorn stops accepting connections and drains in flight requests on
    # SIGTERM before this runs; then pools are closed cleanly.
    log.info("api shutting down")
    await dispose_async_engine()
    await close_async_redis()


def create_app() -> FastAPI:
    app = FastAPI(
        title="Call Events Service",
        version="0.1.0",
        description="Ingests telephony call event webhooks and fans them out to tenant endpoints.",
        lifespan=lifespan,
    )
    app.add_middleware(RequestContextMiddleware)
    for module in (ops, ingest, events, deliveries, endpoints):
        app.include_router(module.router)
    return app


app = create_app()
