"""Idempotent seed: one demo tenant, its API key, provider account mappings and two endpoints.

Safe to run on every container start. The API key comes from DEMO_API_KEY so it is known to
whoever ran `docker compose up`; it is stored only as an Argon2 hash.
"""

import logging
import sys
import uuid

from sqlalchemy import select

from app.config import get_settings
from app.db import sync_session
from app.logs import configure_logging
from app.models import Endpoint, ProviderAccount, Tenant
from app.security import hash_api_key, parse_key_id

log = logging.getLogger("seed")

DEMO_TENANT_ID = uuid.UUID("00000000-0000-4000-8000-000000000001")
DEMO_ACCOUNTS = {"acmetel": "AC_demo", "voxly": "vx_demo"}


def seed() -> None:
    s = get_settings()
    key_id = parse_key_id(s.demo_api_key)
    if key_id is None:
        sys.exit("DEMO_API_KEY must look like ck_<key_id>_<secret>")

    with sync_session() as session:
        tenant = session.get(Tenant, DEMO_TENANT_ID)
        if tenant is None:
            tenant = Tenant(
                id=DEMO_TENANT_ID,
                name="Demo Telecom Ltd",
                api_key_id=key_id,
                api_key_hash=hash_api_key(s.demo_api_key),
            )
            session.add(tenant)
            session.flush()
            log.info("created demo tenant", extra={"tenant_id": str(DEMO_TENANT_ID)})

        for provider, account in DEMO_ACCOUNTS.items():
            exists = session.scalar(
                select(ProviderAccount.id).where(
                    ProviderAccount.provider == provider,
                    ProviderAccount.external_account_id == account,
                )
            )
            if exists is None:
                session.add(
                    ProviderAccount(
                        tenant_id=DEMO_TENANT_ID, provider=provider, external_account_id=account
                    )
                )

        has_endpoints = session.scalar(
            select(Endpoint.id).where(Endpoint.tenant_id == DEMO_TENANT_ID).limit(1)
        )
        if has_endpoints is None:
            base = s.demo_receiver_base_url.rstrip("/")
            session.add_all(
                [
                    Endpoint(
                        tenant_id=DEMO_TENANT_ID,
                        url=f"{base}/hooks/all-events",
                        secret=s.demo_endpoint_secret,
                        event_types=["*"],
                    ),
                    Endpoint(
                        tenant_id=DEMO_TENANT_ID,
                        url=f"{base}/hooks/completed-only",
                        secret=s.demo_endpoint_secret,
                        event_types=["call.completed", "call.failed"],
                    ),
                ]
            )
            log.info("created demo endpoints", extra={"receiver": base})
    log.info("seed complete", extra={"api_key_id": key_id})


if __name__ == "__main__":
    configure_logging(get_settings().log_level)
    seed()
