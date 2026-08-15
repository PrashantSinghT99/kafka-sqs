"""At-least-once Kafka consumer with post-transaction offset commits."""

from __future__ import annotations

from dataclasses import dataclass
import json
from threading import Event
from time import monotonic
from typing import Any, Protocol

from confluent_kafka import Consumer, Message

from order_app.messaging.contracts import parse_order_created_event
from order_app.order_consumer.downstream import DownstreamNotificationError
from order_app.order_consumer.reliability import DeadLetterFailure, RetryPolicy
from order_app.order_consumer.store import EventStoreResult, PostgresOrderStore


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
    attempts: int = 1
    dead_lettered: bool = False


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

    def discard(self, event_id: Any) -> None: ...


class _OrderDownstream(Protocol):
    def notify(self, event: Any) -> None: ...


class _DeadLetterPublisher(Protocol):
    def publish_failure(self, topic: str, failure: DeadLetterFailure) -> Any: ...


class KafkaOrderConsumer:
    """Process one event transactionally, then commit its Kafka offset."""

    def __init__(
        self,
        settings: ConsumerSettings,
        topic: str,
        store: PostgresOrderStore | _OrderStore,
        *,
        downstream: _OrderDownstream | None = None,
        retry_policy: RetryPolicy | None = None,
        dead_letter_publisher: _DeadLetterPublisher | None = None,
        dead_letter_topic: str | None = None,
        consumer: _ConsumerClient | None = None,
    ) -> None:
        if not topic.strip():
            raise ValueError("Kafka topic must not be blank.")
        self.settings = settings
        self.topic = topic
        self.store = store
        self.downstream = downstream
        self.retry_policy = retry_policy or RetryPolicy(
            retryable_errors=(DownstreamNotificationError,)
        )
        if (dead_letter_publisher is None) != (dead_letter_topic is None):
            raise ValueError(
                "dead_letter_publisher and dead_letter_topic must be provided together."
            )
        self.dead_letter_publisher = dead_letter_publisher
        self.dead_letter_topic = dead_letter_topic
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

        value = message.value()
        try:
            payload = json.loads(value.decode("utf-8") if value is not None else "")
            if not isinstance(payload, dict):
                raise TypeError("event JSON must be an object")
            event = parse_order_created_event(payload)
        except Exception as exc:
            if self.dead_letter_publisher is not None:
                return self._dead_letter(
                    message,
                    original_payload=_safe_original_payload(value),
                    error=exc,
                    attempts=1,
                    event=None,
                    duplicate=False,
                )
            raise OrderConsumerError(
                f"Order event validation failed before offset commit at "
                f"{message.topic()}[{message.partition()}]@{message.offset()}: {exc}"
            ) from exc

        attempts = 0
        initial_is_new: bool | None = None
        while True:
            attempts += 1
            try:
                store_result = self.store.store(event)
                if initial_is_new is None:
                    initial_is_new = store_result.is_new
                if store_result.downstream_required:
                    if self.downstream is not None:
                        self.downstream.notify(event)
                    self.store.mark_completed(event.event_id)
                break
            except Exception as exc:
                can_retry = (
                    self.retry_policy.is_retryable(exc)
                    and attempts < self.retry_policy.max_attempts
                )
                if can_retry:
                    Event().wait(self.retry_policy.backoff_seconds)
                    continue
                if self.dead_letter_publisher is not None:
                    self.store.discard(event.event_id)
                    return self._dead_letter(
                        message,
                        original_payload=payload,
                        error=exc,
                        attempts=attempts,
                        event=event,
                        duplicate=initial_is_new is False,
                    )
                raise OrderConsumerError(
                    f"Order event processing failed before offset commit at "
                    f"{message.topic()}[{message.partition()}]@{message.offset()} "
                    f"after {attempts} attempt(s): {exc}"
                ) from exc

        self._commit(message, event_id=str(event.event_id))
        return ProcessedKafkaRecord(
            event_id=str(event.event_id),
            topic=message.topic(),
            partition=message.partition(),
            offset=message.offset(),
            group_id=self.settings.group_id,
            duplicate=initial_is_new is False,
            attempts=attempts,
        )

    def _commit(self, message: Message, *, event_id: str) -> None:
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
                f"Effects completed for event {event_id}, but Kafka offset "
                f"commit failed for {message.topic()}[{message.partition()}]@"
                f"{message.offset()}: {exc}"
            ) from exc

    def _dead_letter(
        self,
        message: Message,
        *,
        original_payload: object,
        error: Exception,
        attempts: int,
        event: Any | None,
        duplicate: bool,
    ) -> ProcessedKafkaRecord:
        assert self.dead_letter_publisher is not None
        assert self.dead_letter_topic is not None
        key_bytes = message.key()
        key = key_bytes.decode("utf-8", errors="replace") if key_bytes else None
        failure = DeadLetterFailure(
            source_topic=message.topic(),
            source_partition=message.partition(),
            source_offset=message.offset(),
            key=key,
            event_id=str(event.event_id) if event is not None else None,
            correlation_id=event.correlation_id if event is not None else None,
            attempts=attempts,
            error_type=type(error).__name__,
            error_message=str(error),
            original_payload=original_payload,
        )
        self.dead_letter_publisher.publish_failure(self.dead_letter_topic, failure)
        event_id = failure.event_id or "unknown"
        self._commit(message, event_id=event_id)
        return ProcessedKafkaRecord(
            event_id=event_id,
            topic=message.topic(),
            partition=message.partition(),
            offset=message.offset(),
            group_id=self.settings.group_id,
            duplicate=duplicate,
            attempts=attempts,
            dead_lettered=True,
        )

    def close(self) -> None:
        if not self._closed:
            self._consumer.close()
            self._closed = True


def _safe_original_payload(value: bytes | None) -> object:
    if value is None:
        return None
    try:
        return json.loads(value.decode("utf-8"))
    except Exception:
        return value.decode("utf-8", errors="replace")
