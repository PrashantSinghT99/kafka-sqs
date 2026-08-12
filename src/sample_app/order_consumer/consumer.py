"""At-least-once Kafka consumer with post-transaction offset commits."""

from __future__ import annotations

from dataclasses import dataclass
import json
from time import monotonic
from typing import Any, Protocol

from confluent_kafka import Consumer, Message

from mqtest.contracts import parse_order_created_event
from sample_app.order_consumer.store import EventStoreResult, PostgresOrderStore


class OrderConsumerError(RuntimeError):
    """Raised when a record cannot be parsed, stored, or committed."""


class OrderConsumerTimeout(TimeoutError):
    """Raised when no Kafka record arrives within the processing deadline."""


@dataclass(frozen=True)
class ConsumerSettings:
    """Explicit at-least-once consumer configuration."""

    bootstrap_servers: str
    group_id: str
    client_id: str = "sample-order-consumer"
    offset_reset: str = "earliest"
    poll_interval_seconds: float = 0.2

    def as_confluent_config(self) -> dict[str, object]:
        if not self.group_id.strip():
            raise ValueError("Consumer group_id must not be blank.")
        if self.offset_reset not in {"earliest", "latest"}:
            raise ValueError("offset_reset must be 'earliest' or 'latest'.")
        if self.poll_interval_seconds <= 0:
            raise ValueError("poll_interval_seconds must be greater than zero.")
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.group_id,
            "client.id": self.client_id,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "auto.offset.reset": self.offset_reset,
            "isolation.level": "read_committed",
        }


@dataclass(frozen=True)
class ProcessedKafkaRecord:
    """Evidence that database processing and the offset commit completed."""

    event_id: str
    topic: str
    partition: int
    offset: int
    group_id: str
    duplicate: bool


class _ConsumerClient(Protocol):
    def subscribe(self, topics: list[str]) -> None: ...

    def poll(self, timeout: float = -1) -> Message | None: ...

    def commit(
        self,
        message: Message | None = None,
        asynchronous: bool = True,
    ) -> list[Any] | None: ...

    def close(self) -> None: ...


class _OrderStore(Protocol):
    def store(self, event: Any) -> EventStoreResult: ...

    def mark_completed(self, event_id: Any) -> None: ...


class _OrderDownstream(Protocol):
    def notify(self, event: Any) -> None: ...


class KafkaOrderConsumer:
    """Process one event transactionally, then commit its Kafka offset."""

    def __init__(
        self,
        settings: ConsumerSettings,
        topic: str,
        store: PostgresOrderStore | _OrderStore,
        *,
        downstream: _OrderDownstream | None = None,
        consumer: _ConsumerClient | None = None,
    ) -> None:
        if not topic.strip():
            raise ValueError("Kafka topic must not be blank.")
        self.settings = settings
        self.topic = topic
        self.store = store
        self.downstream = downstream
        self._consumer = consumer or Consumer(settings.as_confluent_config())
        self._closed = False
        self._consumer.subscribe([topic])

    def __enter__(self) -> KafkaOrderConsumer:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def process_one(self, *, timeout_seconds: float = 10.0) -> ProcessedKafkaRecord:
        """Process one record and commit only after the DB transaction returns."""
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be greater than zero.")
        deadline = monotonic() + timeout_seconds
        message: Message | None = None
        while monotonic() < deadline:
            remaining = deadline - monotonic()
            if remaining <= 0:
                break
            message = self._consumer.poll(
                min(self.settings.poll_interval_seconds, remaining)
            )
            if message is not None:
                break

        if message is None:
            raise OrderConsumerTimeout(
                f"No Kafka record arrived on {self.topic!r} within "
                f"{timeout_seconds:.2f} seconds; group_id={self.settings.group_id!r}."
            )
        if message.error() is not None:
            raise OrderConsumerError(
                f"Kafka consumer error at {message.topic()}["
                f"{message.partition()}]: {message.error()}"
            )

        try:
            value = message.value()
            payload = json.loads(value.decode("utf-8") if value is not None else "")
            if not isinstance(payload, dict):
                raise TypeError("event JSON must be an object")
            event = parse_order_created_event(payload)
            store_result = self.store.store(event)
            if store_result.downstream_required:
                if self.downstream is not None:
                    self.downstream.notify(event)
                self.store.mark_completed(event.event_id)
        except Exception as exc:
            raise OrderConsumerError(
                f"Order event processing failed before offset commit at "
                f"{message.topic()}[{message.partition()}]@{message.offset()}: {exc}"
            ) from exc

        try:
            committed = self._consumer.commit(message=message, asynchronous=False)
            commit_errors = [
                partition.error
                for partition in (committed or [])
                if getattr(partition, "error", None) is not None
            ]
            if commit_errors:
                raise RuntimeError(commit_errors[0])
        except Exception as exc:
            raise OrderConsumerError(
                f"Database committed event {event.event_id}, but Kafka offset "
                f"commit failed for {message.topic()}[{message.partition()}]@"
                f"{message.offset()}: {exc}"
            ) from exc

        return ProcessedKafkaRecord(
            event_id=str(event.event_id),
            topic=message.topic(),
            partition=message.partition(),
            offset=message.offset(),
            group_id=self.settings.group_id,
            duplicate=not store_result.is_new,
        )

    def close(self) -> None:
        if not self._closed:
            self._consumer.close()
            self._closed = True
