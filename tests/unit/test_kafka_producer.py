"""Unit tests for Kafka producer serialization and delivery handling."""

from __future__ import annotations

import json
from typing import Any

import pytest

from mqtest.contracts import make_order_created_event
from mqtest.kafka import (
    KafkaEventProducer,
    KafkaPublishError,
    ProducerSettings,
)


class _DeliveredMessage:
    def topic(self) -> str:
        return "orders"

    def partition(self) -> int:
        return 2

    def offset(self) -> int:
        return 41

    def timestamp(self) -> tuple[int, int]:
        return (1, 1_786_515_200_000)


class _FakeProducer:
    def __init__(self, *, error: object | None = None, remaining: int = 0) -> None:
        self.error = error
        self.remaining = remaining
        self.produced: dict[str, Any] | None = None

    def produce(self, topic: str, **kwargs: Any) -> None:
        self.produced = {"topic": topic, **kwargs}

    def flush(self, timeout: float = -1) -> int:
        assert self.produced is not None
        if self.remaining == 0:
            self.produced["on_delivery"](self.error, _DeliveredMessage())
        return self.remaining


@pytest.mark.unit
def test_producer_settings_enable_strong_delivery_defaults() -> None:
    config = ProducerSettings("kafka:9092").as_confluent_config()

    assert config["enable.idempotence"] is True
    assert config["acks"] == "all"
    assert config["delivery.timeout.ms"] == 15_000
    assert config["request.timeout.ms"] == 5_000
    assert config["linger.ms"] == 0


@pytest.mark.unit
def test_publish_uses_order_key_contract_headers_and_json() -> None:
    fake = _FakeProducer()
    event = make_order_created_event(
        order_id="ORD-900",
        correlation_id="checkout-123",
        causation_id="request-123",
    )
    producer = KafkaEventProducer(
        ProducerSettings("unused:9092"),
        producer=fake,
    )

    result = producer.publish_order_created("orders", event)

    assert fake.produced is not None
    assert fake.produced["key"] == b"ORD-900"
    assert json.loads(fake.produced["value"])["event_id"] == str(event.event_id)
    assert dict(fake.produced["headers"])["correlation-id"] == b"checkout-123"
    assert result.topic == "orders"
    assert result.partition == 2
    assert result.offset == 41
    assert result.key == "ORD-900"


@pytest.mark.unit
def test_delivery_callback_error_becomes_test_friendly_exception() -> None:
    producer = KafkaEventProducer(
        ProducerSettings("unused:9092"),
        producer=_FakeProducer(error="broker rejected record"),
    )

    with pytest.raises(KafkaPublishError, match="broker rejected record"):
        producer.publish_order_created("orders", make_order_created_event())


@pytest.mark.unit
def test_flush_timeout_reports_undelivered_count() -> None:
    producer = KafkaEventProducer(
        ProducerSettings("unused:9092"),
        producer=_FakeProducer(remaining=1),
    )

    with pytest.raises(KafkaPublishError, match="1 queued record"):
        producer.publish_order_created("orders", make_order_created_event())


@pytest.mark.unit
def test_delivery_timeout_must_exceed_request_timeout() -> None:
    settings = ProducerSettings(
        "unused:9092",
        delivery_timeout_seconds=5,
        request_timeout_seconds=5,
    )

    with pytest.raises(ValueError, match="greater than"):
        settings.as_confluent_config()

