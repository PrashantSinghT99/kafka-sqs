"""Reliable Kafka publishing primitives for controlled test events."""

from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any, Protocol

from confluent_kafka import KafkaException, Message, Producer

from order_app.messaging.contracts import OrderCreatedEvent, validate_order_created_contract
from order_app.messaging.contracts.models import event_to_wire_dict


class KafkaPublishError(RuntimeError):
    """Raised when Kafka does not acknowledge a test event successfully."""


@dataclass(frozen=True)
class ProducerSettings:
    """Explicit producer reliability and timeout settings."""

    bootstrap_servers: str
    client_id: str = "order-app-event-producer"
    delivery_timeout_seconds: float = 15.0
    request_timeout_seconds: float = 5.0
    linger_ms: int = 0

    def as_confluent_config(self) -> dict[str, object]:
        if self.delivery_timeout_seconds <= self.request_timeout_seconds:
            raise ValueError(
                "delivery_timeout_seconds must be greater than "
                "request_timeout_seconds."
            )
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": self.client_id,
            "enable.idempotence": True,
            "acks": "all",
            "delivery.timeout.ms": int(self.delivery_timeout_seconds * 1_000),
            "request.timeout.ms": int(self.request_timeout_seconds * 1_000),
            "linger.ms": self.linger_ms,
        }


@dataclass(frozen=True)
class PublishedRecord:
    """Broker acknowledgement returned to a producer test."""

    topic: str
    partition: int
    offset: int
    timestamp_ms: int | None
    key: str
    headers: tuple[tuple[str, bytes], ...]


class _ProducerClient(Protocol):
    def produce(self, topic: str, **kwargs: Any) -> None: ...

    def flush(self, timeout: float = -1) -> int: ...


def serialize_order_created_event(event: OrderCreatedEvent) -> bytes:
    """Validate and serialize one event into deterministic UTF-8 JSON."""
    validate_order_created_contract(event)
    return json.dumps(
        event_to_wire_dict(event),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def order_created_headers(
    event: OrderCreatedEvent,
) -> tuple[tuple[str, bytes], ...]:
    """Build broker headers used for filtering, tracing, and diagnostics."""
    return (
        ("content-type", b"application/json"),
        ("event-type", event.event_type.encode("utf-8")),
        ("event-version", str(event.event_version).encode("ascii")),
        ("event-id", str(event.event_id).encode("ascii")),
        ("correlation-id", event.correlation_id.encode("utf-8")),
        ("causation-id", event.causation_id.encode("utf-8")),
    )


class KafkaEventProducer:
    """Publish a contract-valid event and wait for its delivery report."""

    def __init__(
        self,
        settings: ProducerSettings,
        *,
        producer: _ProducerClient | None = None,
    ) -> None:
        self.settings = settings
        self._producer = producer or Producer(settings.as_confluent_config())

    def publish_order_created(
        self,
        topic: str,
        event: OrderCreatedEvent,
    ) -> PublishedRecord:
        """Publish one event synchronously for deterministic test control."""
        if not topic.strip():
            raise ValueError("Kafka topic must not be blank.")

        key = event.data.order_id
        headers = order_created_headers(event)
        value = serialize_order_created_event(event)
        delivered_message: list[Message] = []
        delivery_errors: list[object] = []

        def on_delivery(error: object | None, message: Message) -> None:
            if error is not None:
                delivery_errors.append(error)
                return
            delivered_message.append(message)

        try:
            self._producer.produce(
                topic,
                key=key.encode("utf-8"),
                value=value,
                headers=list(headers),
                on_delivery=on_delivery,
            )
            undelivered = self._producer.flush(self.settings.delivery_timeout_seconds)
        except (BufferError, KafkaException, RuntimeError) as exc:
            raise KafkaPublishError(
                f"Kafka rejected event {event.event_id} for topic {topic!r}: {exc}"
            ) from exc

        if undelivered:
            raise KafkaPublishError(
                f"Kafka did not acknowledge {undelivered} queued record(s) within "
                f"{self.settings.delivery_timeout_seconds:.1f} seconds; "
                f"event_id={event.event_id}, topic={topic!r}."
            )
        if delivery_errors:
            raise KafkaPublishError(
                f"Kafka delivery failed for event {event.event_id} on topic "
                f"{topic!r}: {delivery_errors[0]}"
            )
        if len(delivered_message) != 1:
            raise KafkaPublishError(
                f"Kafka flush completed without one delivery report for event "
                f"{event.event_id} on topic {topic!r}."
            )

        message = delivered_message[0]
        timestamp_ms = _message_timestamp_ms(message)
        return PublishedRecord(
            topic=message.topic(),
            partition=message.partition(),
            offset=message.offset(),
            timestamp_ms=timestamp_ms,
            key=key,
            headers=headers,
        )


def _message_timestamp_ms(message: Message) -> int | None:
    _, timestamp_ms = message.timestamp()
    return timestamp_ms if timestamp_ms is not None and timestamp_ms >= 0 else None
