"""Two scenarios, run one at a time so their numbers do not blur together:

    locust -f loadtest/locustfile.py --headless -u 50 -r 10 -t 60s IngestUser
    locust -f loadtest/locustfile.py --headless -u 50 -r 10 -t 60s ReadUser

IngestUser posts correctly signed webhooks from both providers, with ~5% exact replays of an
earlier webhook to exercise the dedupe path. By default each user sends as fast as it can (a
saturation test). Set INGEST_RPS_PER_USER to pace users instead (users x rate = offered load),
which is how the steady state delivery lag is measured.
ReadUser pages through GET /v1/events with the cursor, five pages deep, then starts over.

Defaults match the seeded demo tenant; override with env vars of the same names.
"""

import os
import random
from collections import deque

from locust import FastHttpUser, constant, constant_throughput, task

from loadtest.webhooks import acmetel, voxly

ACME_SECRET = os.environ.get("ACMETEL_SIGNING_SECRET", "dev-acmetel-secret")
VOXLY_SECRET = os.environ.get("VOXLY_SIGNING_SECRET", "dev-voxly-secret")
API_KEY = os.environ.get("DEMO_API_KEY", "ck_demo_0123456789abcdef0123456789abcdef")
DUPLICATE_SHARE = float(os.environ.get("DUPLICATE_SHARE", "0.05"))
INGEST_RPS_PER_USER = float(os.environ.get("INGEST_RPS_PER_USER", "0"))

_recent: deque[tuple[str, bytes, dict[str, str]]] = deque(maxlen=500)


class IngestUser(FastHttpUser):
    wait_time = constant_throughput(INGEST_RPS_PER_USER) if INGEST_RPS_PER_USER else constant(0)

    @task
    def ingest(self) -> None:
        if _recent and random.random() < DUPLICATE_SHARE:
            provider, body, headers = random.choice(_recent)
            name = f"/v1/events/{provider} (replay)"
        else:
            provider = random.choice(("acmetel", "voxly"))
            body, headers = acmetel(ACME_SECRET) if provider == "acmetel" else voxly(VOXLY_SECRET)
            name = f"/v1/events/{provider}"
        with self.client.post(
            f"/v1/events/{provider}", data=body, headers=headers, name=name, catch_response=True
        ) as resp:
            if resp.status_code != 202:
                resp.failure(f"status {resp.status_code}: {resp.text[:120]}")
            elif name.endswith("(replay)"):
                if not resp.json().get("duplicate"):
                    resp.failure("replayed webhook was not reported as duplicate")
            else:
                # Only offer it for replay once the original has been accepted; otherwise a
                # replay can overtake the original and the roles swap.
                _recent.append((provider, body, headers))


class ReadUser(FastHttpUser):
    wait_time = constant(0)

    def on_start(self) -> None:
        self.headers = {"Authorization": f"Bearer {API_KEY}"}
        self.cursor: str | None = None
        self.depth = 0

    @task
    def page(self) -> None:
        url = "/v1/events?limit=50"
        name = "/v1/events (first page)"
        if self.cursor:
            url += f"&cursor={self.cursor}"
            name = "/v1/events (cursor page)"
        with self.client.get(url, headers=self.headers, name=name, catch_response=True) as resp:
            if resp.status_code != 200:
                resp.failure(f"status {resp.status_code}")
                self.cursor, self.depth = None, 0
                return
            self.cursor = resp.json().get("next_cursor")
            self.depth += 1
            if self.depth >= 5 or not self.cursor:
                self.cursor, self.depth = None, 0
