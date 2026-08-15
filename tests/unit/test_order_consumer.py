"""Unit tests for database-before-offset consumer orchestration."""

from __future__ import annotations

from typing import Any

import pytest

from order_app.messaging.contracts import make_order_created_event
from order_app.messaging import serialize_order_created_event
from order_app.order_processing import (
    ConsumerSettings,
    EventStoreResult,
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
        self.completed: list[Any] = []
        self.error = error

    def store(self, event: Any) -> EventStoreResult:
        if self.error is not None:
            raise self.error
        self.events.append(event)
        return EventStoreResult(is_new=True, downstream_required=True)

    def mark_completed(self, event_id: Any) -> None:
        self.completed.append(event_id)


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


class _FakeDownstream:
    def __init__(self, *, error: Exception | None = None) -> None:
        self.events: list[Any] = []
        self.error = error

    def notify(self, event: Any) -> None:
        self.events.append(event)
        if self.error is not None:
            raise self.error


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
    assert store.completed == [event.event_id]
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


@pytest.mark.unit
def test_downstream_failure_after_store_prevents_offset_commit() -> None:
    event = make_order_created_event()
    store = _FakeStore()
    downstream = _FakeDownstream(error=RuntimeError("HTTP 503"))
    client = _FakeConsumer(
        _FakeMessage(serialize_order_created_event(event)),
        store,
    )

    with KafkaOrderConsumer(
        ConsumerSettings("unused:9092", "orders-service"),
        "orders",
        store,
        downstream=downstream,
        consumer=client,
    ) as consumer:
        with pytest.raises(OrderConsumerError, match="before offset commit"):
            consumer.process_one(timeout_seconds=0.1)

    assert store.events == [event]
    assert store.completed == []
    assert downstream.events == [event]
    assert client.commits == []


@pytest.mark.unit
def test_completed_duplicate_skips_downstream_and_commits_offset() -> None:
    event = make_order_created_event()
    store = _FakeStore()

    def duplicate_store(observed: Any) -> EventStoreResult:
        store.events.append(observed)
        return EventStoreResult(is_new=False, downstream_required=False)

    store.store = duplicate_store  # type: ignore[method-assign]
    downstream = _FakeDownstream()
    client = _FakeConsumer(
        _FakeMessage(serialize_order_created_event(event)),
        store,
    )

    with KafkaOrderConsumer(
        ConsumerSettings("unused:9092", "orders-service"),
        "orders",
        store,
        downstream=downstream,
        consumer=client,
    ) as consumer:
        processed = consumer.process_one(timeout_seconds=0.1)

    assert processed.duplicate is True
    assert downstream.events == []
    assert store.completed == []
    assert len(client.commits) == 1
