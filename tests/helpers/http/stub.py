"""Disposable real-HTTP stub with queued responses and captured requests."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from threading import Lock, Thread
from typing import Any


@dataclass(frozen=True)
class HttpStubResponse:
    """One response returned to the next request received by the stub."""

    status_code: int = 200
    json_body: object | None = None


@dataclass(frozen=True)
class RecordedHttpRequest:
    """Compact evidence captured at the downstream HTTP boundary."""

    method: str
    path: str
    headers: dict[str, str]
    body: bytes

    def json(self) -> Any:
        return json.loads(self.body.decode("utf-8"))


class RecordingHttpStub:
    """Context-managed HTTP server owned entirely by one test."""

    def __init__(self) -> None:
        self._lock = Lock()
        self._responses: deque[HttpStubResponse] = deque()
        self._requests: list[RecordedHttpRequest] = []
        self._server: ThreadingHTTPServer | None = None
        self._thread: Thread | None = None

    def __enter__(self) -> RecordingHttpStub:
        return self.start()

    def __exit__(self, *_: object) -> None:
        self.stop()

    @property
    def base_url(self) -> str:
        if self._server is None:
            raise RuntimeError("HTTP stub has not been started.")
        host, port = self._server.server_address[:2]
        return f"http://{host}:{port}"

    @property
    def requests(self) -> tuple[RecordedHttpRequest, ...]:
        with self._lock:
            return tuple(self._requests)

    def enqueue_response(
        self,
        status_code: int,
        *,
        json_body: object | None = None,
    ) -> None:
        if not 100 <= status_code <= 599:
            raise ValueError("HTTP stub status_code must be between 100 and 599.")
        with self._lock:
            self._responses.append(HttpStubResponse(status_code, json_body))

    def start(self) -> RecordingHttpStub:
        if self._server is not None:
            return self

        stub = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:  # noqa: N802 - stdlib handler API
                content_length = int(self.headers.get("Content-Length", "0"))
                body = self.rfile.read(content_length)
                request = RecordedHttpRequest(
                    method="POST",
                    path=self.path,
                    headers={key.lower(): value for key, value in self.headers.items()},
                    body=body,
                )
                with stub._lock:
                    stub._requests.append(request)
                    response = (
                        stub._responses.popleft()
                        if stub._responses
                        else HttpStubResponse()
                    )

                payload = (
                    json.dumps(response.json_body).encode("utf-8")
                    if response.json_body is not None
                    else b""
                )
                self.send_response(response.status_code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, *_: object) -> None:
                return

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._thread = Thread(
            target=self._server.serve_forever,
            name="order-app-test-http-stub",
            daemon=True,
        )
        self._thread.start()
        return self

    def stop(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self._server = None
        if self._thread is not None:
            self._thread.join(timeout=2)
            self._thread = None
