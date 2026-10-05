# Call Events Service

Telephony providers send us call event webhooks. This service verifies and stores each event exactly once, then fans it out to every webhook endpoint the owning tenant has subscribed, with signed payloads, retries with backoff, a dead letter table and replay. Tenants read their events and delivery status back over a paginated, rate limited API.

Stack: FastAPI on uvicorn, Postgres 16 through SQLAlchemy 2.0 with Alembic migrations, Celery workers on RabbitMQ, Redis for caching, rate limiting and circuit breaking, pytest, Docker and docker compose, Python 3.12.

## Request lifecycle

```mermaid
sequenceDiagram
    autonumber
    participant P as Provider (AcmeTel / Voxly)
    participant API as API (FastAPI)
    participant PG as Postgres
    participant MQ as RabbitMQ
    participant W as Celery worker
    participant R as Redis
    participant T as Tenant endpoint

    P->>API: POST /v1/events/{provider} (raw body + HMAC header)
    API->>API: verify HMAC SHA256 (constant time), else 401
    API->>API: validate provider shape (Pydantic v2), normalise
    API->>PG: INSERT ... ON CONFLICT (tenant_id, provider_event_id) DO NOTHING
    API->>PG: COMMIT
    alt new event
        API->>MQ: publish fanout_event (after commit)
    end
    API-->>P: 202 {event_id, duplicate}

    MQ->>W: fanout_event
    W->>R: endpoint config (versioned cache)
    W->>PG: INSERT deliveries ON CONFLICT (event_id, endpoint_id) DO NOTHING
    W->>MQ: publish deliver(delivery_id) per new row

    MQ->>W: deliver
    W->>R: circuit breaker admit + per endpoint token bucket (Lua)
    W->>PG: claim: UPDATE ... SET status='in_flight' WHERE status IN ('pending','retrying')
    W->>T: POST body, X-Signature, X-Timestamp, Idempotency-Key (10s timeout)
    alt 2xx
        W->>PG: status = succeeded
    else retryable failure, attempts < 6
        W->>PG: status = retrying, next_retry_at
        W->>MQ: retry on delivery_retries queue after 1, 4, 16, 64, 256s (+/- 20%)
    else attempts exhausted or permanent 4xx
        W->>PG: status = dead, INSERT dead_letters
    end
```

Beat runs three maintenance tasks: a dispatcher that recovers deliveries whose message was lost, a reaper that returns rows stuck `in_flight` after a worker crash, and a sweeper that fans out events whose fanout publish failed after commit.

## Why a queue rather than inline delivery

The provider is waiting on our HTTP response, and providers retry or disable webhooks that respond slowly. If ingestion called tenant endpoints inline, our latency would become the sum of every tenant receiver's latency, one slow or dead receiver would hold ingestion request slots open, and a receiver outage would turn into ingestion failures and provider retries. With a queue the ingest path is two short Postgres statements and a publish, so it returns 202 in tens of milliseconds regardless of what the receivers are doing. The queue also absorbs bursts: in the load test below the API accepted events about ten times faster than the workers could deliver them, and nothing was lost; the backlog drained afterwards. Retries need durable, delayed scheduling, which a broker provides and an HTTP request does not.

The cost is that delivery becomes asynchronous and needs its own idempotency, monitoring and recovery, which is most of the code in `app/services`.

## Idempotency

**Ingestion.** `events` has a unique constraint on `(tenant_id, provider_event_id)`. The insert is `INSERT ... ON CONFLICT DO NOTHING RETURNING id`: a returned id means a new event, no row means a replay, and the response carries `duplicate: true|false`. There is no read before write, so two concurrent copies of the same webhook cannot both insert (one blocks on the unique index, then does nothing); `test_concurrent_duplicates_insert_one_row` fires eight at once and checks for one row. The fanout task is only published for new events and only after the transaction commits, so a worker can always see the row it was told about. If the publish itself fails, the event is still stored with `fanned_out_at` NULL and the sweeper publishes it later, so a broker outage delays delivery rather than losing events.

**Fanout.** `deliveries` has a unique constraint on `(event_id, endpoint_id)` and fanout inserts with `ON CONFLICT DO NOTHING RETURNING id`. Only rows it actually created are enqueued, so running fanout twice for the same event (a redelivered message, the sweeper racing a slow publish) creates and sends nothing extra.

**Delivery.** Celery runs with `acks_late`, so a worker crash redelivers the message, and the dispatcher may enqueue a row again. Neither can cause a concurrent double send because the task claims its row before any network I/O:

```sql
UPDATE deliveries SET status = 'in_flight', attempt_count = attempt_count + 1, claimed_at = now()
WHERE id = :id AND status IN ('pending', 'retrying')
RETURNING attempt_count;
```

and commits. Only one worker can win that update; the rest see no row and exit. `test_concurrent_workers_send_exactly_once` holds six threads at a barrier after they have all read `pending`, releases them together and asserts the receiver was called once; removing the status condition from the UPDATE makes it fail.

What this does not give is exactly once delivery over HTTP, which is not achievable with a third party. If a worker dies after the receiver got the request but before the result is committed, the row stays `in_flight`; after the lease (120s) the reaper returns it to `retrying` and it is sent again. Every request carries `Idempotency-Key: <delivery id>`, stable across retries and replays, so receivers can dedupe. The guarantee is at least once, never concurrently, with a key for receiver side dedupe.

## Why cursor pagination over offset

`OFFSET n` makes Postgres produce and discard n rows, so deep pages get linearly slower, and it is unstable under writes: a row inserted while a client pages shifts everything down one position and the client sees a row twice; a delete makes it skip one. Webhook event tables are append heavy, so that happens constantly.

`GET /v1/events` orders by `(received_at DESC, id DESC)` and the cursor is an opaque `base64url({"t": received_at, "id": id})` of the last row returned. The next page is `WHERE tenant_id = :t AND (received_at, id) < (:ts, :id)`, a row value comparison that walks the `ix_events_tenant_received (tenant_id, received_at DESC, id DESC)` index directly, so page 1000 costs the same as page 1. New events sort before the first page and never shift later pages. The `id` tie breaker makes the order total even when timestamps collide (tested with 30 rows sharing one timestamp). The page fetches `limit + 1` rows to know whether there is a next page without a `COUNT`. `test_cursor_pagination_returns_every_row_once_while_rows_are_inserted` inserts 5 new events between every page fetch and checks that all 137 original rows come back exactly once.

Tradeoffs: clients cannot jump to page N or get a total count. `received_at` uses `clock_timestamp()` at insert, so a transaction that inserts a row and commits slightly later than a concurrent one can, in a narrow window, become visible behind a cursor a client has already passed. At this service's volumes that window is milliseconds; fixing it fully needs a monotonic sequence assigned at commit (see limitations).

## Retry and dead letter policy

| Attempt | Delay before next attempt (nominal) | With jitter |
|---|---|---|
| 1 fails | 1 s | 0.8 to 1.2 s |
| 2 fails | 4 s | 3.2 to 4.8 s |
| 3 fails | 16 s | 12.8 to 19.2 s |
| 4 fails | 64 s | 51 to 77 s |
| 5 fails | 256 s | 205 to 307 s |
| 6 fails | dead letter | |

The delay is `1 * 4^(attempt - 1)` times a uniform jitter factor in `[0.8, 1.2]`, so roughly 5.7 minutes from first attempt to dead letter. Each request has a 10 second timeout.

* Retryable: connection errors, timeouts, 5xx, 408, 425 and 429. A 429 with `Retry-After` waits at least that long (capped at 300 s).
* Not retryable: any other 4xx (400, 401, 404, 410 ...). Those go straight to the dead letter table with reason `non_retryable_status`, on the view that retrying a request the receiver has rejected as malformed or unauthorised five more times only adds load. This is a judgment call; some providers retry everything.
* Mechanism: the delivery task is a Celery task with `autoretry_for=(RetryableDeliveryError,)` and `max_retries=5`. Celery's built in `retry_backoff` is base 2, so a small `Task` subclass feeds our base 4 delay into `retry(countdown=...)`. The attempt count stored in Postgres is the source of truth for dead lettering, and Celery's bound is a backstop that dead letters via `on_failure` if it is ever hit first. Retries go to a separate `delivery_retries` queue so a 1 s retry does not wait behind a backlog of first attempts.
* Dead lettering sets the delivery to `dead`, inserts a `dead_letters` row (attempts, last status code, last error, reason) and logs at error level with the delivery and endpoint ids.
* Replay: `POST /v1/deliveries/{id}/replay` flips a `dead` delivery back to `pending` with `attempt_count = 0` using a conditional UPDATE (so concurrent replays cannot both win; a non dead delivery returns 409), stamps `replayed_at` on the dead letter row and enqueues it. A delivery that dies again gets a second dead letter row, so history is kept.

## Rate limiting and circuit breaking

All of this state lives in Redis and every read modify write is a Lua script, so it is atomic across any number of API processes and workers, and all scripts use the Redis server clock (`TIME`) so hosts with skewed clocks agree.

**Read API, per tenant: sliding window log.** A sorted set per tenant holds one member per request in the last 60 s. The script drops expired members, counts, and either adds the request or rejects it. I chose a sliding log over a fixed window because a fixed window allows twice the limit across a window boundary, and over a token bucket because "N requests per minute" with an exact `X-RateLimit-Remaining` is easier for API clients to reason about. Responses carry `X-RateLimit-Limit`, `X-RateLimit-Remaining` and `X-RateLimit-Reset`; a rejection is `429` with `Retry-After` set to when the oldest request in the window expires. Cost: memory is O(limit) per tenant (600 entries by default), which is fine here and would be swapped for a sliding window counter at much higher limits. Tests cover the exact boundary (5 allowed, the 6th rejected), atomicity (25 concurrent requests against a limit of 10 yield exactly 10 successes) and the window sliding.

**Outbound, per endpoint: token bucket.** Each endpoint has a bucket (default 50 requests per second, burst 100, overridable per endpoint). A delivery that finds no token is deferred to the retry queue with a short delay without claiming the row or spending an attempt. This protects a tenant's receiver from being flooded by a backlog, and keeps one tenant's slow receiver from monopolising workers. In the load test this limiter was the binding constraint on delivery throughput, as intended (see below).

**Circuit breaker, per endpoint.** States are kept in three keys per endpoint (consecutive failure count, `open_until`, a half open probe lock):

* closed: deliveries flow; each retryable failure increments the count and any success resets it.
* open: after 5 consecutive failures the circuit opens for 30 s. Deliveries for that endpoint are deferred without spending attempts, so a receiver outage does not burn through every delivery's retry budget and fill the dead letter table.
* half open: after the cooldown exactly one probe is let through (`SET NX` lock). Success closes the circuit; failure reopens it for another cooldown.

Every transition is written to the `circuit_events` table, counted in `circuit_transitions_total` and logged. Tests walk the whole cycle and check that an open circuit defers without spending attempts.

**Endpoint config cache.** Fanout and delivery read a tenant's active endpoints from Redis. Plain "delete the key on write" has a race: a reader loads old rows, the writer commits and deletes, then the reader writes the stale rows back. Instead each tenant has a version counter; readers cache under `epcache:{tenant}:{version}` and writes `INCR` the version after commit. A slow reader can only fill a key nobody reads again, and the TTL is only a backstop for memory. `test_endpoint_writes_invalidate_cache_immediately` checks that create, update and delete are visible on the next read.

## Load test

**Where and how this was measured.** All numbers are from a single laptop: Intel Core i5 1340P (12 cores, 16 threads), 16 GB RAM, Windows 11. Docker could not run on that machine during this work (see "What was verified" below), so the stack ran as native processes against the same versions of the infrastructure the compose file uses (Postgres 16, Redis, RabbitMQ 4): the API under uvicorn with 4 worker processes, Celery workers with the threads pool and 32 threads each, Celery beat, and the test receiver (20 ms added latency, 10% of requests answered with a 500). Locust ran on the same machine, so the load generator competed with the system under test for CPU, and free memory was around 2 GB throughout. These are numbers for this code on this machine, not a capacity claim for any production setup.

| Run | Scenario | Workers | Duration | Throughput | p50 | p95 | p99 | Error rate |
|---|---|---|---|---|---|---|---|---|
| 1 | Ingestion, saturation (50 users, no wait) | 1 | 90 s | 769 req/s | 60 ms | 100 ms | 140 ms | 0% service errors (see note) |
| 2 | Ingestion, saturation (50 users, no wait) | 4 | 60 s | 474 req/s | 91 ms | 200 ms | 310 ms | 0% |
| 3 | Ingestion, paced at 60 events/s | 4 | 120 s | 60 req/s | 87 ms | 140 ms | 180 ms | 0% |
| 4 | Read, `GET /v1/events?limit=50`, five pages deep per cursor walk, 34k rows | 4 | 60 s | 598 req/s | 82 ms | 150 ms | 180 ms | 0% |

About 5% of ingestion requests were exact replays of an earlier webhook; every one of them in runs 2 and 3 came back `duplicate: true`. Note on run 1: Locust flagged 25 replays (0.04%) as "not reported as duplicate". That was a bug in the load test, not the service: a replay could be sent before the original's response arrived, overtake it, and correctly be the one stored. The locustfile now only replays webhooks that have already been accepted, and runs 2 and 3 show no such failures.

Delivery lag is measured from Postgres as `delivered_at - events.received_at` for every successful delivery, so it includes the fanout hop, queueing, retry backoff and the receiver's 20 ms.

| Run | Events | Deliveries | Dead | Duplicate sends seen by receiver | Lag p50 / p95 / p99 (first attempt successes) | Lag p50 / p95 / p99 (all successes) |
|---|---|---|---|---|---|---|
| 2, saturation | 27,059 | 37,856 | 0 | 0 | 110 s / 145 s / 147 s | 110 s / 146 s / 147 s |
| 3, paced 60/s | 6,818 | 9,573 | 0 | 0 | 252 ms / 333 ms / 373 ms | 260 ms / 1.37 s / 4.9 s |

How to read these:

* **The workers are the bottleneck, by a wide margin.** In run 2 the API accepted 27k events in 60 s; delivering their 37.9k webhooks (42k HTTP attempts including retries) took until 3 min 40 s after the first event. Four worker processes completed roughly 314 tasks per second in total (fanout plus delivery attempts), about 78 per process. The saturation lag numbers are therefore backlog drain time, not a property of a single delivery. A sustained ingest above roughly 120 events per second would grow the backlog without bound on this setup.
* **Why the workers are slow.** Each worker process runs at about 120% CPU, so it is bound by the Python interpreter (the GIL), not by Postgres, Redis or RabbitMQ, which were mostly idle during the drain. Profiling the delivery path shows each delivery making five Redis round trips (two for the endpoint cache, one each for the circuit breaker admit, the token bucket and recording the result), four short Postgres transactions and the HTTP call, for about 15 ms of CPU per delivery. Adding threads does not help; adding processes does, but sublinearly on one laptop (1 to 4 processes raised fanout from about 75 to about 195 per second). The changes that would fix this are listed under limitations.
* **Steady state is fine.** At 60 events per second (about half of measured capacity) a delivery that succeeds first time lands 250 to 370 ms after ingestion. The all successes p95 and p99 of 1.4 s and 4.9 s are the 1 s and 4 s backoff steps showing up for the 10% of requests the receiver fails on purpose, which is the retry policy working as designed.
* **Ingestion latency is mostly the durable publish.** On an idle system a single ingest takes 17 ms end to end, of which about 12 ms is waiting for RabbitMQ to confirm a persistent message. The higher p50 under load (60 to 90 ms) is CPU contention on a machine also running Locust, four workers and all the infrastructure. Run 2's ingestion throughput is lower than run 1's for the same reason: in run 2 four busy workers were competing with the API for the same cores.
* **No double sends under load.** The receiver tracks `Idempotency-Key` on successful requests. Across all runs, including run 1 where a since fixed dispatcher bug put tens of thousands of redundant messages on the queue, it recorded zero duplicate successes and zero bad signatures.
* **Outbound limiter.** Runs 2 and 3 raised the per endpoint outbound limit to 500 per second so the numbers show worker capacity. With the default of 50 per second, run 1 delivered at about 90 per second across the two demo endpoints and recorded 86k rate limit deferrals: the limiter was capping throughput exactly as intended, protecting a receiver from a backlog.

**What the first run found.** Run 1 used the code as first written and exposed three problems, all fixed in commit `eb62932`: the dispatcher treated queued deliveries as lost and enqueued them again (about 50 redundant messages per second during the backlog), retries were republished behind the whole backlog so a 1 s retry waited minutes, and rate limited deliveries were put back on the queue after a few milliseconds and spun against the token bucket. Run 1's delivery figures are not in the table because they measure those bugs.

Raw Locust CSVs and lag reports are in `loadtest/results/`.

## Run locally

```bash
docker compose up --build
```

That is the whole setup. Compose starts Postgres, Redis and RabbitMQ, waits for them to be healthy, runs a one shot `migrate` service (Alembic `upgrade head`, then an idempotent seed), and only then starts the API, the Celery worker, Celery beat and a test receiver. No `.env` file is needed; `.env.example` lists every variable and its default.

The seed creates a demo tenant whose API key is `ck_demo_0123456789abcdef0123456789abcdef` (stored only as an Argon2 hash), maps provider accounts `AC_demo` (AcmeTel) and `vx_demo` (Voxly) to it, and creates two endpoints pointing at the bundled receiver: one subscribed to everything, one to `call.completed` and `call.failed`. The receiver fails 10% of requests on purpose so retries and dead letters happen.

```bash
# send a signed webhook (uses the same helper as the load test)
python -c "
import httpx; from loadtest.webhooks import acmetel
body, headers = acmetel('dev-acmetel-secret')
print(httpx.post('http://localhost:8000/v1/events/acmetel', content=body, headers=headers).json())"

KEY='Authorization: Bearer ck_demo_0123456789abcdef0123456789abcdef'
curl -s -H "$KEY" 'localhost:8000/v1/events?limit=10'
curl -s -H "$KEY" 'localhost:8000/v1/deliveries?status=dead'
curl -s -X POST -H "$KEY" localhost:8000/v1/deliveries/<id>/replay
curl -s localhost:8080/stats          # receiver: requests, injected failures, duplicate successes
curl -s localhost:8000/readyz         # postgres, redis, rabbitmq
curl -s localhost:8000/metrics        # API metrics plus queue depth
curl -s localhost:9100/metrics        # worker metrics (9100 to 9103 when scaled): attempts, failures, lag, circuit transitions
```

OpenAPI docs are at `http://localhost:8000/docs`. RabbitMQ's management UI is at `http://localhost:15672` (callevents / callevents).

**Tests** run against the compose services (the Postgres container creates a `callevents_test` database on first start):

```bash
pip install -e ".[dev]"
pytest -q              # 41 tests, about 20 s
ruff check . && ruff format --check . && mypy app
```

**Load test:**

```bash
pip install -e ".[dev]"
locust -f loadtest/locustfile.py --headless -u 50 -r 25 -t 90s --host http://localhost:8000 IngestUser
INGEST_RPS_PER_USER=1 locust -f loadtest/locustfile.py --headless -u 50 -r 25 -t 120s --host http://localhost:8000 IngestUser
locust -f loadtest/locustfile.py --headless -u 50 -r 25 -t 60s --host http://localhost:8000 ReadUser
python -m loadtest.delivery_lag --since <ISO time the run started> --receiver http://localhost:8080
```

For the read scenario raise `READ_RATE_LIMIT`, otherwise the demo tenant's 600 per minute limit turns most requests into 429s (which is the limiter working, not a throughput number).

## API

| Method | Path | Notes |
|---|---|---|
| POST | `/v1/events/{provider}` | `acmetel` or `voxly`. Provider HMAC, no API key. 202 `{event_id, duplicate}` |
| GET | `/v1/events` | `limit` (1 to 500), `cursor`, `event_type`, `call_id`, `received_after`, `received_before` |
| GET | `/v1/events/{id}` | |
| GET | `/v1/deliveries` | `status`, `event_id`, `limit`, `cursor` |
| GET | `/v1/deliveries/{id}` | |
| POST | `/v1/deliveries/{id}/replay` | dead deliveries only, else 409 |
| POST, GET | `/v1/endpoints` | create returns the signing secret once |
| GET, PATCH, DELETE | `/v1/endpoints/{id}` | writes invalidate the endpoint cache |
| GET | `/healthz`, `/readyz`, `/metrics` | liveness, dependency readiness, Prometheus |

Everything under `/v1` except ingestion needs `Authorization: Bearer <api key>` and is rate limited per tenant. Every query is filtered by the authenticated tenant id inside the handler, and other tenants' ids return 404 rather than 403 so ids cannot be probed. `test_tenant_isolation` gives tenant B a valid key and checks it cannot list, fetch, filter by call id, replay, update or delete anything belonging to tenant A, and that A's data is unchanged afterwards.

**Provider shapes.** AcmeTel sends a flat, Twilio style body (`EventSid`, `AccountSid`, `CallSid`, `CallStatus` ...) signed as `X-Acme-Signature: hex(HMAC_SHA256(secret, body))`. Voxly sends a nested envelope (`id`, `type`, `account.id`, `data.call.uuid` ...) signed Stripe style as `Voxly-Signature: t=<unix>,v1=hex(HMAC_SHA256(secret, t + "." + body))`, and requests older than 5 minutes are rejected. Both normalise to one internal event with one vocabulary (`call.initiated`, `call.ringing`, `call.answered`, `call.completed`, `call.failed`). The signature is verified on the raw bytes before any JSON parsing, using `hmac.compare_digest`. The webhook body's account id maps to a tenant through `provider_accounts`.

**Outbound signature.** Receivers get `X-Signature: hex(HMAC_SHA256(endpoint secret, raw body))`, `X-Timestamp`, `Idempotency-Key`, `X-Event-Id`, `X-Delivery-Attempt` and `X-Request-Id`. The receiver in `receiver/app.py` shows verification.

## Operations

* `/healthz` is liveness only and checks nothing external, so a database outage does not get every API container restarted. `/readyz` checks Postgres (`SELECT 1`), Redis (`PING`) and RabbitMQ (a connection) with a 2 s budget each and returns 503 with per dependency detail.
* Metrics: `http_request_duration_seconds` (by route template, method, status), `events_ingested_total`, `ingest_rejected_total`, `delivery_attempts_total`, `delivery_failures_total` (by reason), `delivery_deferred_total`, `dead_letters_total`, `delivery_lag_seconds` (ingestion to successful delivery), `circuit_transitions_total`, `endpoint_cache_requests_total`, `read_api_rate_limited_total`, and `queue_depth` per queue plus `deliveries_by_status`, sampled at scrape time. The API uses prometheus_client multiprocess mode because uvicorn runs several worker processes; the Celery worker serves its own registry on port 9100.
* Logs are one JSON object per line. A request id is taken from `X-Request-ID` (or generated), returned on the response, attached to every log line, put in a Celery message header on publish and restored in the task, so one id follows a webhook from ingestion through fanout to every delivery attempt and is forwarded to the receiver as `X-Request-Id`.
* Graceful shutdown: the entrypoint `exec`s each process so `SIGTERM` reaches it. Uvicorn stops accepting connections and drains in flight requests (20 s), then the lifespan hook disposes the database pool and Redis client. The Celery worker does a warm shutdown and finishes running tasks (compose allows 60 s, the delivery timeout is 10 s); with `acks_late` anything unfinished is redelivered and the claim makes that safe.

## Design decisions and tradeoffs

* **Celery threads pool, not prefork.** Delivery is I/O bound (waiting on receivers), threads are cheap, and one process per container keeps Prometheus metrics simple. The load test shows the cost: each worker process tops out around one core because of the GIL, so throughput scales by adding worker containers (`docker compose up --scale worker=4`), not threads.
* **asyncpg for the API, psycopg 3 for workers and Alembic.** The API is async; Celery tasks are synchronous. One `DATABASE_URL` is rewritten to each driver.
* **Provider account mapping table.** Ingestion URLs do not contain a tenant id; the tenant is resolved from the account id inside the signed body. That keeps provider configuration to one URL per provider and means a tenant id in a URL can never be spoofed.
* **API key verification cache.** Argon2id verification costs tens of milliseconds by design, which would dominate read latency, so a verified key is cached in Redis for 5 minutes under `sha256(key)` (never the key itself). Revoking a key therefore takes up to 5 minutes unless that cache entry is deleted too.
* **Endpoint signing secrets are stored in plaintext** in Postgres and in the Redis cache because the service must produce HMACs with them. Production would encrypt them with a KMS envelope key.
* **Signature covers the body only**, as specified. Because `X-Timestamp` is not inside the MAC, a receiver cannot use it to reject replayed requests; they should dedupe on `Idempotency-Key` or the event id. A v2 scheme signing `timestamp.body` (like Voxly's inbound one) would fix that.
* **Celery remote control is disabled** (`worker_enable_remote_control=False`, `--without-mingle --without-gossip`). RabbitMQ 4 refuses the transient non exclusive reply queues it uses, and nothing here needs `celery inspect`.
* **Dispatcher only runs when queues are short.** It exists to recover deliveries whose message was lost. During a backlog an overdue row is almost certainly queued, not lost, and the first load run showed the dispatcher adding about 50 redundant messages per second. It now skips while the delivery queues hold more than 1000 messages; the cost is that a genuinely lost message waits until the backlog clears.

## Deployment

Not deployed anywhere yet. Two supported paths, both from the same image:

**Single host with docker compose.** Put the secrets in `.env` (URL safe characters only, they are interpolated into connection URLs) and layer the production override on top:

```bash
cat >> .env <<EOF
POSTGRES_PASSWORD=...
RABBITMQ_PASSWORD=...
ACMETEL_SIGNING_SECRET=...
VOXLY_SIGNING_SECRET=...
EOF
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d --build
```

`docker-compose.prod.yml` sets `APP_ENV=production`, turns the demo seed off, makes compose fail fast if any of those secrets is missing, publishes only the API port (Postgres, Redis, RabbitMQ and the worker metrics port stay on the internal network), drops the RabbitMQ management UI, and leaves the test receiver behind a `test` profile. Put TLS termination in front of port 8000 and keep `/metrics` off the public internet; if the proxy is not on the same host, set `FORWARDED_ALLOW_IPS` so uvicorn trusts its `X-Forwarded-*` headers. With `APP_ENV=production` the app refuses to start while any provider signing secret (or, if seeding, the demo key and endpoint secret) is still at its public development value. Tenants then have to be created directly in the database, since there is no admin API yet.

**A platform such as Railway, Render or Fly.io.** `railway.toml` describes the API service: build from the Dockerfile, run migrations as a pre deploy command, start the `api` role on the platform's `$PORT`, health check `/readyz`. The worker and beat are two more services from the same image started with `entrypoint.sh worker` and `entrypoint.sh beat` (exactly one beat). Postgres and Redis come from the platform's plugins, RabbitMQ from a template service or CloudAMQP. Set `APP_ENV=production`, `SEED_DEMO_DATA=false`, the two provider secrets, `DATABASE_URL`, `REDIS_URL` and `BROKER_URL` on all three services.

The service needs four long running processes (API, worker, beat, RabbitMQ) plus Postgres and Redis. The free tiers of the common platforms do not support that many always on services, and free web services are put to sleep when idle, which is incompatible with a worker consuming a queue and a scheduler that must keep ticking. Running it there means a paid plan.

## Limitations and what I would change

**What was verified and how.**

* The test suite (41 tests) passes against real Postgres 16, Redis and RabbitMQ 4, as do `ruff check`, `ruff format --check`, `mypy app`, and an Alembic upgrade, downgrade and `alembic check` (no drift between models and migrations).
* The full pipeline (signed webhook, dedupe, fanout, signed delivery, retries, metrics, request id propagation into worker logs) was exercised end to end, and the load tests above ran against it.
* The docker compose stack was run from nothing with `docker compose up --build`: migrations and seed ran, every service came up healthy, a signed webhook was ingested, fanned out and delivered to both receiver endpoints, the metrics endpoints reported it, `docker compose stop` gave the worker a warm shutdown, and a restart re-ran the migrate job as a no-op. The production override was run the same way: the seed was skipped, the demo key was rejected, only port 8000 was published, and RabbitMQ kept its durable queues across a container recreate.
* The GitHub Actions workflow has not run yet because the repository has not been pushed.

**What is not production ready.**

* No TLS and no authentication on `/metrics`; both are left to the proxy in front (see Deployment).
* Endpoint secrets are stored unencrypted (see above). API keys are one per tenant with no rotation or scopes. There is no admin API for tenants; the seed script is the only way to create one.
* Endpoint URLs are not checked for SSRF. A tenant can register `http://169.254.169.254/` or an internal hostname and the worker will POST to it. Production needs URL validation plus egress filtering that blocks private ranges after DNS resolution.
* No data retention: events, deliveries and dead letters grow forever.
* The rate limit and breaker fail closed if Redis is down (requests error) rather than degrading. There is no Redis or RabbitMQ high availability in the compose file.
* Alerting is left to whoever scrapes the metrics; there are no dashboards or alert rules in the repo.
* The bundled receiver is a test double, and the providers AcmeTel and Voxly are invented shapes modelled on Twilio and Stripe/Vonage style webhooks. Nothing here has been exercised against a real telephony provider.

**At ten times the volume.**

* Workers first. The measured ceiling is per worker process and CPU bound in Python, about 15 ms of CPU per task split across five Redis round trips, four short Postgres transactions and the HTTP call. I would merge the circuit breaker check and the token bucket into one Lua call, batch the claim and status updates, move delivery to an asyncio worker (httpx async or aiohttp) so one process holds thousands of in flight requests, and run fanout and delivery as separately scaled worker deployments.
* Per endpoint queues, or at least per endpoint concurrency limits in the worker, so one tenant with a huge backlog cannot delay everyone else's deliveries. Today fairness comes only from the per endpoint token bucket and circuit breaker.
* Partition `events` and `deliveries` by time (monthly) with retention, and move delivery history out of the hot table once terminal.
* Replace Celery ETA retries with a delayed retry table polled by the dispatcher (`next_retry_at` is already indexed for it). Celery holds ETA messages in worker memory, which does not scale to large retry backlogs and is lost on restart until redelivery.
* Replace the sliding window log with a sliding window counter (O(1) memory per tenant).
* Use a monotonically increasing sequence assigned at commit (or logical replication / an outbox read in commit order) for the pagination key, which removes the small late commit visibility window described above, and gives a natural basis for a streaming API.
* Postgres connection pooling with PgBouncer in transaction mode; during the load test the API and four worker processes held about 140 Postgres connections.

## License

MIT, see [LICENSE](LICENSE).
