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
    """Create one valid event while allowing deterministic test overrides."""
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
