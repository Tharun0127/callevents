import asyncio
import json
import time

import httpx
import respx

from app.models import Delivery, Event
from loadtest.webhooks import acmetel, voxly
from tests.conftest import TenantFixture
from tests.helpers import count

ACME = "dev-acmetel-secret"
VOX = "dev-voxly-secret"


async def test_duplicate_webhook_inserts_one_row(
    api: httpx.AsyncClient, tenant: TenantFixture, receiver: respx.MockRouter
) -> None:
    receiver.post("/hook").respond(200)
    body, headers = acmetel(ACME, account=tenant.acme_account, event_id="EVdup1")

    first = await api.post("/v1/events/acmetel", content=body, headers=headers)
    second = await api.post("/v1/events/acmetel", content=body, headers=headers)

    assert first.status_code == second.status_code == 202
    assert first.json()["duplicate"] is False
    assert second.json() == {"event_id": first.json()["event_id"], "duplicate": True}
    assert count(Event) == 1
    # The replay must not fan out again either.
    assert count(Delivery) == 1
    assert len(receiver.calls) == 1


async def test_concurrent_duplicates_insert_one_row(
    api: httpx.AsyncClient, tenant: TenantFixture, receiver: respx.MockRouter
) -> None:
    receiver.post("/hook").respond(200)
    body, headers = voxly(VOX, account=tenant.voxly_account, event_id="evt_race")

    responses = await asyncio.gather(
        *[api.post("/v1/events/voxly", content=body, headers=headers) for _ in range(8)]
    )

    assert all(r.status_code == 202 for r in responses)
    assert sum(1 for r in responses if not r.json()["duplicate"]) == 1
    assert len({r.json()["event_id"] for r in responses}) == 1
    assert count(Event) == 1


async def test_same_provider_event_id_for_two_tenants_is_not_a_duplicate(
    api: httpx.AsyncClient, receiver: respx.MockRouter
) -> None:
    from tests.conftest import make_tenant

    receiver.post("/hook").respond(200)
    a, b = make_tenant("ta"), make_tenant("tb")
    for t in (a, b):
        body, headers = acmetel(ACME, account=t.acme_account, event_id="EVshared")
        r = await api.post("/v1/events/acmetel", content=body, headers=headers)
        assert r.json()["duplicate"] is False
    assert count(Event) == 2


async def test_tampered_body_is_rejected(api: httpx.AsyncClient, tenant: TenantFixture) -> None:
    body, headers = acmetel(ACME, account=tenant.acme_account)
    doc = json.loads(body)
    doc["CallDuration"] = 9999
    tampered = json.dumps(doc).encode()

    r = await api.post("/v1/events/acmetel", content=tampered, headers=headers)

    assert r.status_code == 401
    assert count(Event) == 0


async def test_tampered_voxly_body_is_rejected(
    api: httpx.AsyncClient, tenant: TenantFixture
) -> None:
    body, headers = voxly(VOX, account=tenant.voxly_account)
    r = await api.post("/v1/events/voxly", content=body + b" ", headers=headers)
    assert r.status_code == 401
    assert count(Event) == 0


async def test_wrong_secret_missing_header_and_stale_timestamp_are_rejected(
    api: httpx.AsyncClient, tenant: TenantFixture
) -> None:
    body, headers = acmetel("not-the-secret", account=tenant.acme_account)
    assert (await api.post("/v1/events/acmetel", content=body, headers=headers)).status_code == 401

    body, _ = acmetel(ACME, account=tenant.acme_account)
    r = await api.post(
        "/v1/events/acmetel", content=body, headers={"Content-Type": "application/json"}
    )
    assert r.status_code == 401

    # Voxly signs the timestamp; a correctly signed but old request is a replay.
    import hashlib
    import hmac

    body, _ = voxly(VOX, account=tenant.voxly_account)
    old = str(int(time.time()) - 3600)
    sig = hmac.new(VOX.encode(), old.encode() + b"." + body, hashlib.sha256).hexdigest()
    r = await api.post(
        "/v1/events/voxly", content=body, headers={"Voxly-Signature": f"t={old},v1={sig}"}
    )
    assert r.status_code == 401
    assert count(Event) == 0


async def test_both_providers_normalise_to_one_shape(
    api: httpx.AsyncClient, tenant: TenantFixture, receiver: respx.MockRouter
) -> None:
    receiver.post("/hook").respond(200)
    for provider, (body, headers) in (
        ("acmetel", acmetel(ACME, account=tenant.acme_account)),
        ("voxly", voxly(VOX, account=tenant.voxly_account)),
    ):
        r = await api.post(f"/v1/events/{provider}", content=body, headers=headers)
        assert r.status_code == 202, r.text
        ev = (await api.get(f"/v1/events/{r.json()['event_id']}", headers=tenant.headers)).json()
        assert ev["provider"] == provider
        assert ev["event_type"].startswith("call.")
        assert {"from", "to", "direction", "duration_seconds", "raw"} <= ev["payload"].keys()


async def test_invalid_shape_unknown_provider_and_unknown_account(
    api: httpx.AsyncClient, tenant: TenantFixture
) -> None:
    import hashlib
    import hmac

    bad = b'{"EventSid": "EV1"}'
    sig = hmac.new(ACME.encode(), bad, hashlib.sha256).hexdigest()
    r = await api.post("/v1/events/acmetel", content=bad, headers={"X-Acme-Signature": sig})
    assert r.status_code == 422

    assert (await api.post("/v1/events/nope", content=b"{}")).status_code == 404

    body, headers = acmetel(ACME, account="AC_nobody")
    assert (await api.post("/v1/events/acmetel", content=body, headers=headers)).status_code == 404
    assert count(Event) == 0


async def test_ingest_returns_202_even_if_broker_publish_fails(
    api: httpx.AsyncClient, tenant: TenantFixture, monkeypatch: object
) -> None:
    import pytest

    from app.api import ingest

    def boom(_: object) -> None:
        raise ConnectionError("broker down")

    mp = pytest.MonkeyPatch()
    mp.setattr(ingest, "_enqueue_fanout", boom)
    try:
        body, headers = acmetel(ACME, account=tenant.acme_account)
        r = await api.post("/v1/events/acmetel", content=body, headers=headers)
    finally:
        mp.undo()
    assert r.status_code == 202
    # Stored but not fanned out: the sweeper will find it.
    from app.services.fanout import unfanned_events

    # Negative age gives a margin for clock granularity between Python and Postgres.
    assert len(unfanned_events(older_than_seconds=-5)) == 1
