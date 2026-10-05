"""Report delivery lag (event received -> delivery succeeded) and delivery outcomes from Postgres.

    python -m loadtest.delivery_lag --since 2026-10-05T12:00:00Z [--receiver http://localhost:8080]

Lag is measured from the database timestamps, so it includes queueing, fanout, any retry
backoff and the receiver's response time. It is reported separately for deliveries that
succeeded on the first attempt and for all successful deliveries.
"""

import argparse
import json
import urllib.request

from sqlalchemy import text

from app.db import sync_session

QUERY = text(
    """
    SELECT
      count(*) FILTER (WHERE d.status = 'succeeded') AS succeeded,
      count(*) FILTER (WHERE d.status = 'dead') AS dead,
      count(*) FILTER (WHERE d.status IN ('pending', 'retrying', 'in_flight')) AS outstanding,
      count(*) FILTER (WHERE d.status = 'succeeded' AND d.attempt_count = 1) AS first_try,
      percentile_cont(ARRAY[0.5, 0.95, 0.99]) WITHIN GROUP (
        ORDER BY extract(epoch FROM d.delivered_at - e.received_at)
      ) FILTER (WHERE d.status = 'succeeded') AS lag_all,
      percentile_cont(ARRAY[0.5, 0.95, 0.99]) WITHIN GROUP (
        ORDER BY extract(epoch FROM d.delivered_at - e.received_at)
      ) FILTER (WHERE d.status = 'succeeded' AND d.attempt_count = 1) AS lag_first_try,
      max(extract(epoch FROM d.delivered_at - e.received_at)) AS lag_max
    FROM deliveries d JOIN events e ON e.id = d.event_id
    WHERE e.received_at >= :since
    """
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--since", required=True)
    parser.add_argument("--receiver", default=None)
    args = parser.parse_args()

    with sync_session() as session:
        row = session.execute(QUERY, {"since": args.since}).mappings().one()
        events = session.execute(
            text("SELECT count(*) FROM events WHERE received_at >= :since"), {"since": args.since}
        ).scalar_one()

    def fmt(values: list[float] | None) -> str:
        if not values:
            return "n/a"
        return " / ".join(f"{v * 1000:.0f} ms" for v in values)

    print(f"events ingested:         {events}")
    print(f"deliveries succeeded:    {row['succeeded']} (first attempt: {row['first_try']})")
    print(f"deliveries dead:         {row['dead']}")
    print(f"deliveries outstanding:  {row['outstanding']}")
    print(f"lag p50/p95/p99, first attempt: {fmt(row['lag_first_try'])}")
    print(f"lag p50/p95/p99, all successes: {fmt(row['lag_all'])}")
    print(f"lag max: {row['lag_max'] or 0:.1f} s")
    if args.receiver:
        with urllib.request.urlopen(f"{args.receiver}/stats", timeout=5) as r:  # noqa: S310
            print("receiver:", json.dumps(json.load(r)))


if __name__ == "__main__":
    main()
