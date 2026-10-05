#!/bin/sh
# Every role execs its process so SIGTERM from `docker stop` reaches it directly:
# uvicorn drains in flight requests, Celery does a warm shutdown and finishes running tasks.
set -eu

role="${1:-api}"

case "$role" in
  migrate)
    alembic upgrade head
    exec python -m scripts.seed
    ;;
  api)
    export PROMETHEUS_MULTIPROC_DIR="${PROMETHEUS_MULTIPROC_DIR:-/tmp/prometheus}"
    rm -rf "$PROMETHEUS_MULTIPROC_DIR" && mkdir -p "$PROMETHEUS_MULTIPROC_DIR"
    exec uvicorn app.main:app --host 0.0.0.0 --port 8000 \
      --workers "${API_WORKERS:-2}" \
      --timeout-graceful-shutdown "${API_GRACEFUL_SECONDS:-20}" \
      --no-access-log --proxy-headers
    ;;
  worker)
    exec celery -A app.celery_app worker \
      --pool=threads --concurrency="${WORKER_CONCURRENCY:-32}" \
      -Q fanout,deliveries,maintenance \
      --without-mingle --without-gossip --loglevel="${LOG_LEVEL:-INFO}"
    ;;
  beat)
    exec celery -A app.celery_app beat -s /tmp/celerybeat-schedule --loglevel="${LOG_LEVEL:-INFO}"
    ;;
  receiver)
    exec uvicorn receiver.app:app --host 0.0.0.0 --port 8080 --no-access-log
    ;;
  *)
    exec "$@"
    ;;
esac
