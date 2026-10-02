"""Проверка параллельности consumer'а на поднятом compose.

    uv run python scripts/throughput.py [N]

Создает N платежей разом и ждет, пока все станут финальными. Обработка
одного платежа занимает 2-5 с (в среднем 3,5 с), поэтому оценка
параллельности = N * 3.5 / время. Около 1 - consumer обрабатывает
сообщения последовательно, около CONSUMER_PREFETCH - параллельно.
"""

import os
import sys
import time
import uuid
from pathlib import Path

import httpx

API = os.environ.get("E2E_API_URL", "http://localhost:8000")
AVERAGE_PROCESSING_SECONDS = 3.5


def api_key() -> str:
    if key := os.environ.get("API_KEY"):
        return key
    for line in (Path(__file__).resolve().parents[1] / ".env").read_text().splitlines():
        if line.startswith("API_KEY="):
            return line.split("=", 1)[1].strip()
    sys.exit("API_KEY is not set")


def main(count: int) -> None:
    headers = {"X-API-Key": api_key()}
    client = httpx.Client(base_url=API, headers=headers, timeout=10)
    started = time.monotonic()
    ids = [
        client.post(
            "/api/v1/payments",
            headers={"Idempotency-Key": str(uuid.uuid4())},
            json={
                "amount": "10.00",
                "currency": "RUB",
                "description": "throughput",
                "webhook_url": "http://webhook-sink:9000/ok",
            },
        ).json()["payment_id"]
        for _ in range(count)
    ]
    pending = set(ids)
    while pending and time.monotonic() - started < 300:
        pending = {
            pid
            for pid in pending
            if client.get(f"/api/v1/payments/{pid}").json()["status"] == "pending"
        }
        time.sleep(0.5)

    elapsed = time.monotonic() - started
    if pending:
        sys.exit(f"{len(pending)} of {count} payments still pending after {elapsed:.0f}s")
    parallelism = count * AVERAGE_PROCESSING_SECONDS / elapsed
    print(f"{count} payments processed in {elapsed:.1f}s, parallelism ~{parallelism:.1f}")


if __name__ == "__main__":
    main(int(sys.argv[1]) if len(sys.argv) > 1 else 20)
