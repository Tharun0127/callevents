import asyncio
import uuid
from datetime import UTC, datetime, timedelta

import httpx
import respx

from app.config import get_settings
from app.models import DeliveryStatus
from tests.conftest import TenantFixture, make_tenant
from tests.helpers import insert_delivery, insert_event


async def _collect(
    api: httpx.AsyncClient, tenant: TenantFixture, limit: int, between=None
) -> list[str]:
    ids: list[str] = []
    cursor: str | None = None
    page_no = 0
    while True:
        params: dict[str, str | int] = {"limit": limit}
        if cursor:
            params["cursor"] = cursor
        r = await api.get("/v1/events", params=params, headers=tenant.headers)
        assert r.status_code == 200, r.text
        body = r.json()
        ids.extend(e["id"] for e in body["data"])
        cursor = body["next_cursor"]
        page_no += 1
        if between:
            between(page_no)
        if not cursor:
            return ids


async def test_cursor_pagination_returns_every_row_once_while_rows_are_inserted(
    api: httpx.AsyncClient, tenant: TenantFixture
) -> None:
    get_settings().read_rate_limit = 10_000
    original = {str(insert_event(tenant.id)) for _ in range(137)}
    inserted_during: list[str] = []

    def insert_more(_: int) -> None:
        for _ in range(5):
            inserted_during.append(str(insert_event(tenant.id)))

    seen = await _collect(api, tenant, limit=20, between=insert_more)

    assert len(seen) == len(set(seen)), "a row was returned twice"
    assert original <= set(seen), "a row that existed before paging began was skipped"
    # New rows land before page one (newest first) so they never appear mid traversal.
    assert not set(inserted_during) & set(seen)


async def test_pagination_tie_break_on_identical_timestamps(
    api: httpx.AsyncClient, tenant: TenantFixture
) -> None:
    get_settings().read_rate_limit = 10_000
    ts = datetime(2026, 1, 1, tzinfo=UTC)
    same = {str(insert_event(tenant.id, received_at=ts)) for _ in range(30)}
    seen = await _collect(api, tenant, limit=7)
    assert sorted(seen) == sorted(same)


async def test_event_filters(api: httpx.AsyncClient, tenant: TenantFixture) -> None:
    now = datetime.now(UTC)
    insert_event(
        tenant.id, event_type="call.completed", call_id="CA1", received_at=now - timedelta(hours=2)
    )
    insert_event(
        tenant.id, event_type="call.failed", call_id="CA1", received_at=now - timedelta(hours=1)
    )
    insert_event(tenant.id, event_type="call.completed", call_id="CA2", received_at=now)

    async def ids(**params: str) -> int:
        r = await api.get("/v1/events", params=params, headers=tenant.headers)
        return len(r.json()["data"])

    assert await ids(event_type="call.completed") == 2
    assert await ids(call_id="CA1") == 2
    assert await ids(call_id="CA1", event_type="call.failed") == 1
    assert await ids(received_after=(now - timedelta(minutes=90)).isoformat()) == 2
    assert await ids(received_before=(now - timedelta(minutes=90)).isoformat()) == 1


async def test_invalid_cursor_is_400(api: httpx.AsyncClient, tenant: TenantFixture) -> None:
    for bad in ("not-base64!!", "eyJ0IjogMX0", "e30"):
        r = await api.get("/v1/events", params={"cursor": bad}, headers=tenant.headers)
        assert r.status_code == 400


async def test_rate_limiter_returns_429_at_the_boundary(
    api: httpx.AsyncClient, tenant: TenantFixture
) -> None:
    get_settings().read_rate_limit = 5
    get_settings().read_rate_window_seconds = 60
    other = make_tenant("other")

    remaining = []
    for _ in range(5):
        r = await api.get("/v1/events", headers=tenant.headers)
        assert r.status_code == 200
        assert r.headers["X-RateLimit-Limit"] == "5"
        remaining.append(int(r.headers["X-RateLimit-Remaining"]))
    assert remaining == [4, 3, 2, 1, 0]

    blocked = await api.get("/v1/events", headers=tenant.headers)
    assert blocked.status_code == 429
    assert 1 <= int(blocked.headers["Retry-After"]) <= 60
    assert blocked.headers["X-RateLimit-Remaining"] == "0"
    assert "X-RateLimit-Reset" in blocked.headers

    # Limits are per tenant.
    assert (await api.get("/v1/events", headers=other.headers)).status_code == 200


async def test_rate_limiter_is_atomic_under_concurrency(
    api: httpx.AsyncClient, tenant: TenantFixture
) -> None:
    get_settings().read_rate_limit = 10
    results = await asyncio.gather(
        *[api.get("/v1/events", headers=tenant.headers) for _ in range(25)]
    )
    codes = [r.status_code for r in results]
    assert codes.count(200) == 10
    assert codes.count(429) == 15


async def test_rate_limit_window_slides(api: httpx.AsyncClient, tenant: TenantFixture) -> None:
    get_settings().read_rate_limit = 2
    get_settings().read_rate_window_seconds = 1
    assert (await api.get("/v1/events", headers=tenant.headers)).status_code == 200
    assert (await api.get("/v1/events", headers=tenant.headers)).status_code == 200
    assert (await api.get("/v1/events", headers=tenant.headers)).status_code == 429
    await asyncio.sleep(1.1)
    assert (await api.get("/v1/events", headers=tenant.headers)).status_code == 200


async def test_auth_rejects_missing_malformed_and_wrong_keys(
    api: httpx.AsyncClient, tenant: TenantFixture
) -> None:
    assert (await api.get("/v1/events")).status_code == 401
    assert (
        await api.get("/v1/events", headers={"Authorization": "Bearer nope"})
    ).status_code == 401
    wrong = tenant.api_key[:-4] + "0000"
    assert (
        await api.get("/v1/events", headers={"Authorization": f"Bearer {wrong}"})
    ).status_code == 401
    # Second call is served from the verification cache and must still succeed.
    assert (await api.get("/v1/events", headers=tenant.headers)).status_code == 200
    assert (await api.get("/v1/events", headers=tenant.headers)).status_code == 200


async def test_tenant_isolation(api: httpx.AsyncClient, receiver: respx.MockRouter) -> None:
    """Tenant B holds a valid key but must not see or touch anything of tenant A's."""
    a, b = make_tenant("a"), make_tenant("b")
    a_event = insert_event(a.id, call_id="CA_secret")
    a_delivery = insert_delivery(a.id, a_event, a.endpoint_ids[0], status=DeliveryStatus.DEAD)
    insert_event(b.id)

    listed = (await api.get("/v1/events", params={"limit": 500}, headers=b.headers)).json()["data"]
    assert [e["id"] for e in listed] != [] and str(a_event) not in {e["id"] for e in listed}
    by_call = await api.get("/v1/events", params={"call_id": "CA_secret"}, headers=b.headers)
    assert by_call.json()["data"] == []

    assert (await api.get(f"/v1/events/{a_event}", headers=b.headers)).status_code == 404
    assert (await api.get(f"/v1/deliveries/{a_delivery}", headers=b.headers)).status_code == 404
    deliveries = (await api.get("/v1/deliveries", headers=b.headers)).json()["data"]
    assert deliveries == []
    assert (
        await api.post(f"/v1/deliveries/{a_delivery}/replay", headers=b.headers)
    ).status_code == 404

    a_ep = a.endpoint_ids[0]
    assert (await api.get(f"/v1/endpoints/{a_ep}", headers=b.headers)).status_code == 404
    patch = await api.patch(f"/v1/endpoints/{a_ep}", json={"active": False}, headers=b.headers)
    assert patch.status_code == 404
    assert (await api.delete(f"/v1/endpoints/{a_ep}", headers=b.headers)).status_code == 404
    b_eps = (await api.get("/v1/endpoints", headers=b.headers)).json()
    assert {e["id"] for e in b_eps} == {str(e) for e in b.endpoint_ids}

    # And A still sees its own data, untouched.
    assert (await api.get(f"/v1/events/{a_event}", headers=a.headers)).status_code == 200
    assert (await api.get(f"/v1/endpoints/{a_ep}", headers=a.headers)).json()["active"] is True


async def test_unknown_ids_are_404(api: httpx.AsyncClient, tenant: TenantFixture) -> None:
    rid = uuid.uuid4()
    assert (await api.get(f"/v1/events/{rid}", headers=tenant.headers)).status_code == 404
    assert (
        await api.post(f"/v1/deliveries/{rid}/replay", headers=tenant.headers)
    ).status_code == 404
