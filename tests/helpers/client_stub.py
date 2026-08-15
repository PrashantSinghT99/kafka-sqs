"""Client-side async assertions and a disposable downstream HTTP stub."""

from __future__ import annotations

from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from threading import Event, Lock, Thread
from time import monotonic
from typing import Any, Generic, TypeVar


ObservedT = TypeVar("ObservedT")


class EventuallyTimeout(AssertionError, Generic[ObservedT]):
    """Assertion failure containing the final bounded-poll evidence."""

    def __init__(
        self,
        *,
        description: str,
        attempts: int,
        elapsed_seconds: float,
        last_observed: ObservedT,
    ) -> None:
        self.description = description
        self.attempts = attempts
        self.elapsed_seconds = elapsed_seconds
        self.last_observed = last_observed
        super().__init__(
            f"Eventually assertion failed for {description!r} after {attempts} "
            f"attempt(s) and {elapsed_seconds:.3f} seconds; "
            f"last_observed={last_observed!r}."
        )


def eventually(
    observe: Callable[[], ObservedT],
    predicate: Callable[[ObservedT], bool],
    *,
    timeout_seconds: float = 10.0,
    interval_seconds: float = 0.1,
    description: str = "expected condition",
) -> ObservedT:
    """Poll an asynchronous outcome until it matches or reaches a deadline.

    Args:
        observe: Function that reads the current external state.
        predicate: Function returning ``True`` when that state is acceptable.
        timeout_seconds: Overall wait limit.
        interval_seconds: Delay between observations.
        description: Human-readable condition included in timeout evidence.

    Returns:
        The first observed value accepted by ``predicate``.

    Raises:
        ValueError: If either timing value is not positive.
        EventuallyTimeout: If no observation matches before the deadline.
    """
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be greater than zero.")
    if interval_seconds <= 0:
        raise ValueError("interval_seconds must be greater than zero.")

    started = monotonic()
    deadline = started + timeout_seconds
    attempts = 0
    last_observed: ObservedT
    waiter = Event()

    while True:
        attempts += 1
        last_observed = observe()
        if predicate(last_observed):
            return last_observed

        remaining = deadline - monotonic()
        if remaining <= 0:
            break
        waiter.wait(min(interval_seconds, remaining))

    raise EventuallyTimeout(
        description=description,
        attempts=attempts,
        elapsed_seconds=monotonic() - started,
        last_observed=last_observed,
    )


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
        """Decode the captured HTTP request body as JSON.

        Returns:
            The decoded JSON value.
        """
        return json.loads(self.body.decode("utf-8"))


class RecordingHttpStub:
    """Run a temporary HTTP server with scripted responses and request capture."""

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
        """Return the running stub's loopback URL.

        Returns:
            URL such as ``http://127.0.0.1:54321``.

        Raises:
            RuntimeError: If the stub has not been started.
        """
        if self._server is None:
            raise RuntimeError("HTTP stub has not been started.")
        host, port = self._server.server_address[:2]
        return f"http://{host}:{port}"

    @property
    def requests(self) -> tuple[RecordedHttpRequest, ...]:
        """Read an immutable snapshot of captured HTTP requests.

        Returns:
            Requests in the order the stub received them.
        """
        with self._lock:
            return tuple(self._requests)

    def enqueue_response(
        self,
        status_code: int,
        *,
        json_body: object | None = None,
    ) -> None:
        """Queue the response returned to the next HTTP request.

        Args:
            status_code: HTTP status from 100 through 599.
            json_body: Optional value serialized as the response body.

        Returns:
            None. The response is appended to the stub's queue.

        Raises:
            ValueError: If ``status_code`` is outside the HTTP range.
        """
        if not 100 <= status_code <= 599:
            raise ValueError("HTTP stub status_code must be between 100 and 599.")
        with self._lock:
            self._responses.append(HttpStubResponse(status_code, json_body))

    def start(self) -> RecordingHttpStub:
        """Start the loopback HTTP server when not already running.

        Returns:
            This stub instance, ready to use as a context manager.
        """
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
        """Stop the server and join its background thread.

        Returns:
            None. Calling it more than once is safe.
        """
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self._server = None
        if self._thread is not None:
            self._thread.join(timeout=2)
            self._thread = None
