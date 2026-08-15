"""Factory for valid `order.created` events."""

from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID, uuid4

from order_app.messaging.contracts.models import (
    OrderCreatedData,
    OrderCreatedEvent,
    ensure_utc_datetime,
)


def make_order_created_event(
    *,
    order_id: str = "ORD-101",
    customer_id: str = "CUS-22",
    amount: float = 500.0,
    currency: str = "INR",
    event_id: UUID | None = None,
    correlation_id: str | None = None,
    causation_id: str | None = None,
    occurred_at: datetime | None = None,
) -> OrderCreatedEvent:
    """Create a valid ``order.created`` event.

    Args:
        order_id: Business order identifier used as the Kafka key.
        customer_id: Customer that owns the order.
        amount: Positive monetary amount.
        currency: Three-letter uppercase currency code.
        event_id: Optional fixed event identity; generated when omitted.
        correlation_id: Optional workflow identity; generated when omitted.
        causation_id: Optional triggering-action identity; generated when omitted.
        occurred_at: Optional timezone-aware timestamp; current UTC when omitted.

    Returns:
        A validated, typed version-1 ``OrderCreatedEvent``.

    Raises:
        ValueError: If ``occurred_at`` has no timezone.
    """
    timestamp = ensure_utc_datetime(occurred_at or datetime.now(timezone.utc))
    return OrderCreatedEvent(
        event_id=event_id or uuid4(),
        occurred_at=timestamp,
        correlation_id=correlation_id or f"checkout-{uuid4()}",
        causation_id=causation_id or f"request-{uuid4()}",
        data=OrderCreatedData(
            order_id=order_id,
            customer_id=customer_id,
            amount=amount,
            currency=currency,
        ),
    )
