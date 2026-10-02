"""Сквозная проверка поднятого docker compose окружения.

    docker compose --profile e2e up -d --build
    uv run python scripts/e2e.py

Проверяет авторизацию, идемпотентность, обработку платежа, доставку
webhook, ретраи с попаданием в DLQ, немедленный DLQ для отказа получателя,
диагностику в DLQ и защиту от SSRF.
"""

import json
import os
import sys
import time
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx

API = os.environ.get("E2E_API_URL", "http://localhost:8000")
SINK = os.environ.get("E2E_SINK_URL", "http://localhost:9000")
RABBIT_API = os.environ.get("E2E_RABBIT_API", "http://localhost:15672/api")
RABBIT_AUTH = ("payments", "payments")
SINK_IN_NETWORK = "http://webhook-sink:9000"
DLQ = "payments.new.dlq"


def env_value(name: str) -> str:
    if value := os.environ.get(name):
        return value
    env = Path(__file__).resolve().parents[1] / ".env"
    for line in env.read_text().splitlines():
        if line.startswith(f"{name}="):
            return line.split("=", 1)[1].strip()
    return ""


def api_key() -> str:
    return env_value("API_KEY") or sys.exit("API_KEY is not set and not found in .env")


def check(condition: bool, message: str) -> None:
    if not condition:
        print(f"  FAIL  {message}")
        sys.exit(1)
    print(f"  ok    {message}")


def wait_for(probe: Callable[[], Any], what: str, within: float = 30.0) -> Any:
    deadline = time.monotonic() + within
    while time.monotonic() < deadline:
        if result := probe():
            return result
        time.sleep(0.5)
    check(False, f"timed out waiting for {what}")


def dlq_depth(client: httpx.Client) -> int:
    response = client.get(f"{RABBIT_API}/queues/%2F/{DLQ}", auth=RABBIT_AUTH)
    response.raise_for_status()
    data = response.json()
    return int(data.get("messages_ready", 0)) + int(data.get("messages_unacknowledged", 0))


def dead_letter_headers(client: httpx.Client, payment_id: str) -> dict[str, Any] | None:
    """Заголовки сообщения платежа в DLQ (чтение без удаления из очереди)."""
    response = client.post(
        f"{RABBIT_API}/queues/%2F/{DLQ}/get",
        auth=RABBIT_AUTH,
        json={"count": 1000, "ackmode": "ack_requeue_true", "encoding": "auto"},
    )
    response.raise_for_status()
    for item in response.json():
        try:
            body = json.loads(item["payload"])
        except (ValueError, TypeError):
            continue
        if body.get("payment_id") == payment_id:
            return dict(item["properties"].get("headers") or {})
    return None


def deliveries(client: httpx.Client, payment_id: str) -> list[dict[str, Any]]:
    items = client.get(f"{SINK}/received").json()
    return [r for r in items if (r.get("body") or {}).get("payment_id") == payment_id]


def main() -> None:
    allowed = {h.strip() for h in env_value("WEBHOOK_ALLOWED_HOSTS").split(",")}
    if "webhook-sink" not in allowed:
        sys.exit("add WEBHOOK_ALLOWED_HOSTS=webhook-sink to .env and recreate the consumer")
    headers = {"X-API-Key": api_key()}
    client = httpx.Client(timeout=10)

    def create(
        path: str, key: str | None = None, amount: str = "1250.50", base: str = SINK_IN_NETWORK
    ) -> httpx.Response:
        return client.post(
            f"{API}/api/v1/payments",
            headers={**headers, "Idempotency-Key": key or str(uuid.uuid4())},
            json={
                "amount": amount,
                "currency": "RUB",
                "description": "e2e",
                "metadata": {"run": "e2e"},
                "webhook_url": f"{base}{path}",
            },
        )

    def payment(payment_id: str) -> dict[str, Any]:
        response = client.get(f"{API}/api/v1/payments/{payment_id}", headers=headers)
        response.raise_for_status()
        return dict(response.json())

    print("auth")
    check(client.get(f"{API}/api/v1/payments/{uuid.uuid4()}").status_code == 401, "no key -> 401")
    check(client.get(f"{API}/health/live").status_code == 401, "health without key -> 401")
    check(client.get(f"{API}/health/ready", headers=headers).status_code == 200, "health -> 200")

    print("idempotency")
    key = str(uuid.uuid4())
    first, replay = create("/ok", key), create("/ok", key)
    check(first.status_code == 202 and first.json()["status"] == "pending", "create -> 202 pending")
    check(replay.json()["payment_id"] == first.json()["payment_id"], "replay -> same payment")
    check(replay.headers.get("Idempotent-Replayed") == "true", "replay is marked")
    check(create("/ok", key, amount="1.00").status_code == 409, "same key, other body -> 409")

    print("processing and webhook")
    payment_id = first.json()["payment_id"]
    done = wait_for(
        lambda: (p := payment(payment_id))["status"] != "pending" and p, "final status"
    )
    check(done["status"] in {"succeeded", "failed"}, f"final status: {done['status']}")
    check(done["processed_at"] is not None, "processed_at is set")
    [hook] = wait_for(lambda: deliveries(client, payment_id), "webhook delivery")
    check(hook["body"]["status"] == done["status"], "webhook carries final status")
    check(hook["headers"].get("idempotency-key") is not None, "webhook has Idempotency-Key")

    print("retries -> DLQ (receiver answers 500)")
    depth = dlq_depth(client)
    failing_id = create("/fail").json()["payment_id"]
    wait_for(lambda: len(deliveries(client, failing_id)) >= 3, "three webhook attempts")
    wait_for(lambda: dlq_depth(client) > depth, "message in DLQ")
    attempts = deliveries(client, failing_id)
    check(len(attempts) == 3, "exactly 3 delivery attempts")
    check(
        len({a["headers"].get("idempotency-key") for a in attempts}) == 1,
        "same Idempotency-Key on every attempt",
    )
    check(payment(failing_id)["status"] != "pending", "payment charged once, status kept")
    dead = wait_for(lambda: dead_letter_headers(client, failing_id), "DLQ diagnostics")
    check(dead.get("x-error-type") == "WebhookTemporaryError", "DLQ: error type recorded")
    check(int(dead.get("x-attempt", 0)) == 3, "DLQ: attempt number recorded")

    print("permanent rejection -> DLQ without retries (receiver answers 404)")
    depth = dlq_depth(client)
    rejected_id = create("/reject").json()["payment_id"]
    wait_for(lambda: dlq_depth(client) > depth, "message in DLQ")
    time.sleep(4)  # окно, в которое пришли бы ретраи, если бы они были
    check(len(deliveries(client, rejected_id)) == 1, "single delivery attempt")
    dead = wait_for(lambda: dead_letter_headers(client, rejected_id), "DLQ diagnostics")
    check(dead.get("x-error-type") == "WebhookRejectedError", "DLQ: rejection recorded")

    print("SSRF: webhook to an internal address is refused")
    ssrf_id = create("/ok", base="http://127.0.0.1:9000").json()["payment_id"]
    dead = wait_for(lambda: dead_letter_headers(client, ssrf_id), "SSRF payment in DLQ")
    check(dead.get("x-error-type") == "WebhookRejectedError", "internal webhook rejected")
    check("non-public" in str(dead.get("x-dead-letter-reason")), "reason names the address")
    check(payment(ssrf_id)["status"] != "pending", "payment itself is processed")

    print("\nall e2e checks passed")


if __name__ == "__main__":
    main()
