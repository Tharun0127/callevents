import logging
from typing import Any

from celery import Celery, signals
from kombu import Queue

from app.config import get_settings
from app.logs import configure_logging, request_id_var

settings = get_settings()

celery_app = Celery("callevents", broker=settings.broker_url, include=["app.tasks"])

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    task_ignore_result=True,
    # Ack after the task body returns, so a worker crash redelivers the message. The delivery
    # row claim (conditional UPDATE) is what stops the redelivered message double sending.
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=4,
    broker_connection_retry_on_startup=True,
    # Separate queues so a backlog of retries cannot starve fanout of fresh events.
    task_queues=(
        Queue("fanout", durable=True),
        Queue("deliveries", durable=True),
        Queue("maintenance", durable=True),
    ),
    task_default_queue="deliveries",
    task_routes={
        "app.tasks.fanout_event": {"queue": "fanout"},
        "app.tasks.deliver": {"queue": "deliveries"},
        "app.tasks.dispatch_due_deliveries": {"queue": "maintenance"},
        "app.tasks.reap_stuck_deliveries": {"queue": "maintenance"},
        "app.tasks.sweep_unfanned_events": {"queue": "maintenance"},
    },
    beat_schedule={
        "dispatch-due-deliveries": {"task": "app.tasks.dispatch_due_deliveries", "schedule": 10.0},
        "reap-stuck-deliveries": {"task": "app.tasks.reap_stuck_deliveries", "schedule": 30.0},
        "sweep-unfanned-events": {"task": "app.tasks.sweep_unfanned_events", "schedule": 15.0},
    },
    worker_hijack_root_logger=False,
    # Remote control (celery inspect, mingle, gossip) declares transient non exclusive reply
    # queues, which RabbitMQ 4 refuses by default. Nothing here depends on it, so it is off.
    worker_enable_remote_control=False,
    worker_cancel_long_running_tasks_on_connection_loss=True,
    # Publisher confirms: apply_async does not return until RabbitMQ has the message.
    broker_transport_options={"confirm_publish": True},
)


@signals.setup_logging.connect
def _setup_logging(**_: Any) -> None:
    configure_logging(settings.log_level)


@signals.before_task_publish.connect
def _propagate_request_id(headers: dict[str, Any] | None = None, **_: Any) -> None:
    rid = request_id_var.get()
    if rid and headers is not None and "request_id" not in headers:
        headers["request_id"] = rid


@signals.task_prerun.connect
def _bind_request_id(task: Any = None, **_: Any) -> None:
    rid = getattr(task.request, "request_id", None) if task is not None else None
    if rid is None and task is not None:
        rid = (task.request.headers or {}).get("request_id") if task.request.headers else None
    request_id_var.set(rid)


@signals.task_postrun.connect
def _unbind_request_id(**_: Any) -> None:
    request_id_var.set(None)


@signals.worker_init.connect
def _start_metrics_server(**_: Any) -> None:
    from prometheus_client import start_http_server

    try:
        start_http_server(settings.worker_metrics_port)
    except OSError:
        logging.getLogger(__name__).warning(
            "worker metrics port busy", extra={"port": settings.worker_metrics_port}
        )


@signals.worker_shutting_down.connect
def _on_shutdown(sig: str = "", how: str = "", **_: Any) -> None:
    logging.getLogger(__name__).info("worker shutting down", extra={"signal": sig, "how": how})
