"""Real HTTP adapter for the order consumer's downstream notification."""

from __future__ import annotations

import httpx2

from order_app.messaging.contracts import OrderCreatedEvent


class DownstreamNotificationError(RuntimeError):
    """Raised when the downstream service does not accept a notification."""


class OrderNotificationClient:
    """Send correlation-aware order notifications over HTTP.

    Args:
        base_url: Downstream service root URL.
        timeout_seconds: Timeout for each HTTP attempt.
        max_attempts: Maximum number of immediate delivery attempts.
    """

    def __init__(
        self,
        base_url: str,
        *,
        timeout_seconds: float = 2.0,
        max_attempts: int = 1,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be greater than zero.")
        if max_attempts <= 0:
            raise ValueError("max_attempts must be greater than zero.")
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.max_attempts = max_attempts

    def notify(self, event: OrderCreatedEvent) -> None:
        """POST one ``order.created`` notification downstream.

        Args:
            event: Typed event used for the body and tracing headers.

        Returns:
            None after a successful 2xx response.

        Raises:
            DownstreamNotificationError: If every HTTP attempt fails.
        """
        last_error: httpx2.HTTPError | None = None
        for _ in range(self.max_attempts):
            try:
                response = httpx2.post(
                    f"{self.base_url}/order-created",
                    headers={
                        "Content-Type": "application/json",
                        "X-Correlation-ID": event.correlation_id,
                        "X-Event-ID": str(event.event_id),
                    },
                    json={
                        "order_id": event.data.order_id,
                        "customer_id": event.data.customer_id,
                        "amount": event.data.amount,
                        "currency": event.data.currency,
                    },
                    timeout=self.timeout_seconds,
                )
                response.raise_for_status()
                return
            except httpx2.HTTPError as exc:
                last_error = exc

        raise DownstreamNotificationError(
            f"Downstream order notification failed after {self.max_attempts} "
            f"attempt(s) for event {event.event_id}: {last_error}"
        ) from last_error
