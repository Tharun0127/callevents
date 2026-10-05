"""Prometheus metrics.

The API runs several uvicorn worker processes, so metrics use prometheus_client multiprocess
mode when PROMETHEUS_MULTIPROC_DIR is set. The Celery worker uses the threads pool (one process)
and serves its own registry on WORKER_METRICS_PORT. Queue depth and delivery status counts are
sampled at scrape time by a custom collector in app.api.ops.
"""

from prometheus_client import Counter, Histogram

HTTP_LATENCY = Histogram(
    "http_request_duration_seconds",
    "HTTP request latency",
    ["method", "route", "status"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)

EVENTS_INGESTED = Counter("events_ingested_total", "Webhooks accepted", ["provider", "duplicate"])
INGEST_REJECTED = Counter("ingest_rejected_total", "Webhooks rejected", ["provider", "reason"])

DELIVERY_ATTEMPTS = Counter("delivery_attempts_total", "Outbound delivery attempts", ["outcome"])
DELIVERY_FAILURES = Counter("delivery_failures_total", "Failed delivery attempts", ["reason"])
DELIVERY_DEFERRED = Counter(
    "delivery_deferred_total", "Deliveries deferred without an attempt", ["reason"]
)
DEAD_LETTERS = Counter(
    "dead_letters_total", "Deliveries moved to the dead letter table", ["reason"]
)
DELIVERY_LAG = Histogram(
    "delivery_lag_seconds",
    "Time from event ingestion to successful delivery",
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120, 300, 600),
)
CIRCUIT_TRANSITIONS = Counter(
    "circuit_transitions_total", "Circuit breaker state changes", ["state"]
)
CACHE_REQUESTS = Counter("endpoint_cache_requests_total", "Endpoint cache lookups", ["result"])
RATE_LIMITED = Counter("read_api_rate_limited_total", "Read API requests rejected with 429")
