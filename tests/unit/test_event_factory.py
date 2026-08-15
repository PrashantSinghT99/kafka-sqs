"""Unit tests for deterministic event-factory overrides."""

from datetime import datetime, timezone
from uuid import UUID

import pytest

from order_app.messaging.contracts import make_order_created_event


@pytest.mark.unit
def test_factory_preserves_supplied_identifiers_and_timestamp() -> None:
    event_id = UUID("11111111-1111-4111-8111-111111111111")
    occurred_at = datetime(2026, 8, 12, 12, 0, tzinfo=timezone.utc)

    event = make_order_created_event(
        event_id=event_id,
        correlation_id="checkout-123",
        causation_id="request-123",
        occurred_at=occurred_at,
    )

    assert event.event_id == event_id
    assert event.correlation_id == "checkout-123"
    assert event.causation_id == "request-123"
    assert event.occurred_at == occurred_at


@pytest.mark.unit
def test_factory_rejects_timestamp_without_timezone() -> None:
    with pytest.raises(ValueError, match="timezone"):
        make_order_created_event(occurred_at=datetime(2026, 8, 12, 12, 0))

