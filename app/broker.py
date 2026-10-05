"""Small helpers that talk to RabbitMQ directly (readiness, queue depth)."""

from kombu import Connection

from app.config import get_settings

QUEUES = ("fanout", "deliveries", "delivery_retries", "maintenance")
_TIMEOUT = 2.0


def broker_ping() -> None:
    with Connection(get_settings().broker_url, connect_timeout=_TIMEOUT) as conn:
        conn.ensure_connection(max_retries=1)


def queue_depths(queues: tuple[str, ...] = QUEUES) -> dict[str, int]:
    """Messages ready per queue, via passive declare (does not create anything)."""
    depths: dict[str, int] = {}
    with Connection(get_settings().broker_url, connect_timeout=_TIMEOUT) as conn:
        channel = conn.channel()
        for q in queues:
            try:
                _, count, _ = channel.queue_declare(queue=q, passive=True)  # type: ignore[attr-defined]
                depths[q] = int(count)
            except Exception:
                # Passive declare of a missing queue closes the channel; reopen and move on.
                depths[q] = 0
                channel = conn.channel()
    return depths
