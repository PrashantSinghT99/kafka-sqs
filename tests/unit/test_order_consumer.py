"""Unit tests for database-before-offset consumer orchestration."""

from __future__ import annotations

from typing import Any

import pytest

from mqtest.contracts import make_order_created_event
from mqtest.kafka import serialize_order_created_event
from sample_app.order_consumer import (
    ConsumerSettings,
    KafkaOrderConsumer,
    OrderConsumerError,
)


class _FakeMessage:
    def __init__(self, value: bytes) -> None:
        self._value = value

    def error(self) -> None:
        return None

    def topic(self) -> str:
        return "orders"

    def partition(self) -> int:
        return 2

    def offset(self) -> int:
        return 19

    def value(self) -> bytes:
        return self._value


class _FakeStore:
    def __init__(self, *, error: Exception | None = None) -> None:
        self.events: list[Any] = []
        self.error = error

    def store(self, event: Any) -> None:
        if self.error is not None:
            raise self.error
        self.events.append(event)


class _FakeConsumer:
    def __init__(self, message: _FakeMessage, store: _FakeStore) -> None:
        self.message = message
        self.store = store
        self.subscriptions: list[str] = []
        self.commits: list[tuple[_FakeMessage, bool]] = []
        self.closed = False

    def subscribe(self, topics: list[str]) -> None:
        self.subscriptions = topics

    def poll(self, timeout: float = -1) -> _FakeMessage | None:
        message, self.message = self.message, None  # type: ignore[assignment]
        return message

    def commit(
        self,
        message: _FakeMessage | None = None,
        asynchronous: bool = True,
    ) -> list[Any]:
        assert self.store.events, "offset committed before database store"
        assert message is not None
        self.commits.append((message, asynchronous))
        return []

    def close(self) -> None:
        self.closed = True


@pytest.mark.unit
def test_consumer_config_disables_automatic_offset_progress() -> None:
    config = ConsumerSettings("kafka:9092", "orders-service").as_confluent_config()

    assert config["enable.auto.commit"] is False
    assert config["enable.auto.offset.store"] is False
    assert config["auto.offset.reset"] == "earliest"
    assert config["isolation.level"] == "read_committed"


@pytest.mark.unit
def test_database_store_completes_before_synchronous_offset_commit() -> None:
    event = make_order_created_event()
    store = _FakeStore()
    client = _FakeConsumer(
        _FakeMessage(serialize_order_created_event(event)),
        store,
    )

    with KafkaOrderConsumer(
        ConsumerSettings("unused:9092", "orders-service"),
        "orders",
        store,
        consumer=client,
    ) as consumer:
        processed = consumer.process_one(timeout_seconds=0.1)

    assert store.events == [event]
    assert len(client.commits) == 1
    assert client.commits[0][1] is False
    assert processed.event_id == str(event.event_id)
    assert processed.offset == 19
    assert client.closed is True


@pytest.mark.unit
def test_database_failure_prevents_offset_commit() -> None:
    event = make_order_created_event()
    store = _FakeStore(error=RuntimeError("database unavailable"))
    client = _FakeConsumer(
        _FakeMessage(serialize_order_created_event(event)),
        store,
    )

    with KafkaOrderConsumer(
        ConsumerSettings("unused:9092", "orders-service"),
        "orders",
        store,
        consumer=client,
    ) as consumer:
        with pytest.raises(OrderConsumerError, match="before offset commit"):
            consumer.process_one(timeout_seconds=0.1)

    assert client.commits == []
