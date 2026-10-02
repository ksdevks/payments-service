"""Тестовый получатель webhook для e2e-проверки (только для профиля e2e).

POST /ok -> 200, /fail -> 500, /reject -> 404. Все запросы записываются,
GET /received отдает их списком.
"""

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

STATUSES = {"/ok": 200, "/fail": 500, "/reject": 404}
received: list[dict[str, Any]] = []
lock = threading.Lock()


class Handler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length)
        status = STATUSES.get(self.path, 404)
        with lock:
            received.append(
                {
                    "path": self.path,
                    "status": status,
                    "headers": {k.lower(): v for k, v in self.headers.items()},
                    "body": json.loads(raw) if raw else None,
                }
            )
        self.send_response(status)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self) -> None:
        if self.path != "/received":
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        with lock:
            payload = json.dumps(received).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


if __name__ == "__main__":
    print("webhook sink listening on :9000", flush=True)
    ThreadingHTTPServer(("0.0.0.0", 9000), Handler).serve_forever()
