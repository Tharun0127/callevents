import logging
import uuid
from typing import Any

from celery import Task

from app.broker import queue_depths
from app.celery_app import celery_app
from app.config import get_settings
from app.services import delivery as delivery_service
from app.services import fanout as fanout_service
from app.services.delivery import OutcomeKind

log = logging.getLogger(__name__)


RETRY_QUEUE = "delivery_retries"


class RetryableDeliveryError(Exception):
    def __init__(self, retry_in: float, reason: str = "") -> None:
        super().__init__(f"delivery failed ({reason}), retry in {retry_in:.2f}s")
        self.retry_in = retry_in


class DeliveryTask(Task):
    """Celery autoretry with our own schedule.

    Celery's built in retry_backoff is base 2 (1, 2, 4, 8...). We want base 4 with jitter
    (roughly 1, 4, 16, 64, 256s), computed from the attempt count stored in Postgres. The task
    body raises RetryableDeliveryError carrying that delay and this override feeds it to
    retry() as the countdown. max_retries bounds Celery's side; the database attempt count is
    what decides dead lettering, so the two agree on 6 total attempts.
    """

    autoretry_for = (RetryableDeliveryError,)
    max_retries = get_settings().delivery_max_attempts - 1
    retry_backoff = False
    retry_jitter = False
    acks_late = True

    def retry(
        self,
        args: Any = None,
        kwargs: Any = None,
        exc: Exception | None = None,
        throw: bool = True,
        eta: Any = None,
        countdown: float | None = None,
        max_retries: int | None = None,
        **options: Any,
    ) -> Any:
        if countdown is None and eta is None and isinstance(exc, RetryableDeliveryError):
            countdown = exc.retry_in
        options.setdefault("queue", RETRY_QUEUE)
        return super().retry(
            args=args,
            kwargs=kwargs,
            exc=exc,
            throw=throw,
            eta=eta,
            countdown=countdown,
            max_retries=max_retries,
            **options,
        )

    def on_failure(
        self, exc: BaseException, task_id: str, args: Any, kwargs: Any, einfo: Any
    ) -> None:
        # Backstop: if Celery gives up before our attempt counter does, still dead letter.
        if args:
            moved = delivery_service.dead_letter_by_id(uuid.UUID(str(args[0])), "task_failed")
            log.error(
                "delivery task failed",
                extra={"delivery_id": str(args[0]), "error": repr(exc), "dead_lettered": moved},
            )


@celery_app.task(name="app.tasks.fanout_event", acks_late=True)
def fanout_event(event_id: str) -> int:
    new_ids = fanout_service.fanout_event(uuid.UUID(event_id))
    for delivery_id in new_ids:
        deliver.delay(str(delivery_id))
    return len(new_ids)


@celery_app.task(base=DeliveryTask, bind=True, name="app.tasks.deliver")
def deliver(self: DeliveryTask, delivery_id: str) -> str:
    outcome = delivery_service.attempt_delivery(uuid.UUID(delivery_id))
    if outcome.kind is OutcomeKind.RETRY:
        raise RetryableDeliveryError(outcome.delay, outcome.detail)
    if outcome.kind is OutcomeKind.DEFERRED:
        # Circuit open or endpoint rate limited: not a failed attempt, so re-enqueue rather
        # than spend one of the bounded retries.
        self.apply_async((delivery_id,), countdown=outcome.delay, queue=RETRY_QUEUE)
    return outcome.kind.value


@celery_app.task(name="app.tasks.dispatch_due_deliveries")
def dispatch_due_deliveries() -> int:
    try:
        depths = queue_depths(("deliveries", RETRY_QUEUE))
    except Exception:
        log.warning("dispatcher could not read queue depth; skipping this tick")
        return 0
    backlog = sum(depths.values())
    if backlog > get_settings().delivery_dispatch_max_queue_depth:
        log.info("dispatcher skipped, queues backed up", extra={"backlog": backlog})
        return 0
    ids = fanout_service.due_overdue_deliveries()
    for delivery_id in ids:
        deliver.delay(str(delivery_id))
    if ids:
        log.info("dispatched overdue deliveries", extra={"count": len(ids)})
    return len(ids)


@celery_app.task(name="app.tasks.reap_stuck_deliveries")
def reap_stuck_deliveries() -> int:
    ids = fanout_service.reap_stuck_in_flight()
    for delivery_id in ids:
        deliver.delay(str(delivery_id))
    return len(ids)


@celery_app.task(name="app.tasks.sweep_unfanned_events")
def sweep_unfanned_events() -> int:
    ids = fanout_service.unfanned_events()
    for event_id in ids:
        fanout_event.delay(str(event_id))
    if ids:
        log.warning("re-enqueued fanout for unfanned events", extra={"count": len(ids)})
    return len(ids)
