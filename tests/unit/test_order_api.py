"""Broker-free tests for request validation and event mapping."""

from __future__ import annotations

from typing import Any

import pytest
from fastapi.testclient import TestClient

from order_app.messaging.contracts import OrderCreatedEvent
from order_app.messaging import EventPublishError
from order_app.order_api import create_order_app


class _RecordingPublisher:
    def __init__(self, error: Exception | None = None) -> None:
        self.calls: list[tuple[str, OrderCreatedEvent]] = []
        self.error = error

    def publish_order_created(
        self,
        topic: str,
        event: OrderCreatedEvent,
    ) -> Any:
        self.calls.append((topic, event))
        if self.error is not None:
            raise self.error
        return None


def _client(publisher: _RecordingPublisher) -> TestClient:
    return TestClient(
        create_order_app(
            publisher,
            "orders.created",
            order_id_factory=lambda: "ORD-API-1",
            correlation_id_factory=lambda: "checkout-generated",
            causation_id_factory=lambda: "request-api-1",
        )
    )


@pytest.mark.unit
def test_valid_request_maps_to_event_and_propagates_correlation_id() -> None:
    publisher = _RecordingPublisher()
    client = _client(publisher)

    response = client.post(
        "/orders",
        headers={"X-Correlation-ID": "checkout-client-1"},
        json={"customer_id": "CUS-7", "amount": 125.5, "currency": "INR"},
    )

    assert response.status_code == 202
    assert response.headers["X-Correlation-ID"] == "checkout-client-1"
    assert response.json()["order_id"] == "ORD-API-1"
    assert response.json()["correlation_id"] == "checkout-client-1"
    assert response.json()["event_id"]
    assert len(publisher.calls) == 1
    topic, event = publisher.calls[0]
    assert topic == "orders.created"
    assert event.correlation_id == "checkout-client-1"
    assert event.causation_id == "request-api-1"
    assert event.data.model_dump() == {
        "order_id": "ORD-API-1",
        "customer_id": "CUS-7",
        "amount": 125.5,
        "currency": "INR",
    }


@pytest.mark.unit
def test_missing_correlation_header_creates_and_returns_one() -> None:
    publisher = _RecordingPublisher()

    response = _client(publisher).post(
        "/orders",
        json={"customer_id": "CUS-8", "amount": 25.0, "currency": "USD"},
    )

    assert response.status_code == 202
    assert response.json()["correlation_id"] == "checkout-generated"
    assert response.headers["X-Correlation-ID"] == "checkout-generated"
    assert publisher.calls[0][1].correlation_id == "checkout-generated"


@pytest.mark.unit
@pytest.mark.parametrize(
    "payload",
    [
        {"amount": 10.0, "currency": "INR"},
        {"customer_id": "CUS-1", "amount": 0.0, "currency": "INR"},
        {"customer_id": "CUS-1", "amount": 10.0, "currency": "inr"},
        {
            "customer_id": "CUS-1",
            "amount": 10.0,
            "currency": "INR",
            "unexpected": True,
        },
    ],
)
def test_invalid_request_never_calls_publisher(payload: dict[str, object]) -> None:
    publisher = _RecordingPublisher()

    response = _client(publisher).post("/orders", json=payload)

    assert response.status_code == 422
    assert publisher.calls == []


@pytest.mark.unit
def test_publish_failure_returns_service_unavailable_without_broker_detail() -> None:
    publisher = _RecordingPublisher(EventPublishError("secret broker detail"))

    response = _client(publisher).post(
        "/orders",
        json={"customer_id": "CUS-9", "amount": 10.0, "currency": "EUR"},
    )

    assert response.status_code == 503
    assert response.json() == {"detail": "Order event could not be published."}
    assert "secret broker detail" not in response.text
