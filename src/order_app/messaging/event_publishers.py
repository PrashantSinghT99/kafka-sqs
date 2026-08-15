"""Shared event-publisher contract with Kafka and SQS implementations."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
import json
from typing import Any, Generic, Protocol, TypeVar

from botocore.exceptions import BotoCoreError, ClientError
from confluent_kafka import KafkaException, Message, Producer

from order_app.messaging.contracts import OrderCreatedEvent, validate_order_created_contract
from order_app.messaging.contracts.models import event_to_wire_dict


ReceiptT = TypeVar("ReceiptT")


class EventPublishError(RuntimeError):
    """Raised when Kafka or SQS rejects or fails to acknowledge an event."""


class EventPublisher(ABC, Generic[ReceiptT]):
    """Define the operation shared by Kafka and SQS event publishers."""

    @abstractmethod
    def publish_order_created(
        self,
        destination: str,
        event: OrderCreatedEvent,
    ) -> ReceiptT:
        """Publish one order event to a broker destination.

        Args:
            destination: Kafka topic name or SQS queue URL.
            event: Typed order event to validate and publish.

        Returns:
            Broker-specific acknowledgement details.

        Raises:
            EventPublishError: If the broker rejects or does not acknowledge it.
        """


@dataclass(frozen=True)
class KafkaPublisherConfig:
    """Hold Kafka connection, reliability, and timeout settings."""

    bootstrap_servers: str
    client_id: str = "order-app-event-producer"
    delivery_timeout_seconds: float = 15.0
    request_timeout_seconds: float = 5.0
    linger_ms: int = 0

    def as_confluent_config(self) -> dict[str, object]:
        """Convert the typed settings to confluent-kafka configuration.

        Returns:
            Configuration dictionary accepted by ``confluent_kafka.Producer``.

        Raises:
            ValueError: If delivery timeout is not longer than request timeout.
        """
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
class KafkaPublishReceipt:
    """Kafka delivery acknowledgement returned to the caller."""

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
    """Validate and serialize an order event as deterministic UTF-8 JSON.

    Args:
        event: Typed event to validate and serialize.

    Returns:
        UTF-8 encoded JSON bytes ready for Kafka.
    """
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
    """Build Kafka headers used for contract identity and tracing.

    Args:
        event: Event whose metadata should become headers.

    Returns:
        Ordered ``(header_name, header_bytes)`` pairs.
    """
    return (
        ("content-type", b"application/json"),
        ("event-type", event.event_type.encode("utf-8")),
        ("event-version", str(event.event_version).encode("ascii")),
        ("event-id", str(event.event_id).encode("ascii")),
        ("correlation-id", event.correlation_id.encode("utf-8")),
        ("causation-id", event.causation_id.encode("utf-8")),
    )


class KafkaEventPublisher(EventPublisher[KafkaPublishReceipt]):
    """Publish validated events to Kafka and wait for acknowledgement.

    Args:
        settings: Kafka connection and delivery configuration.
        producer: Optional compatible producer supplied by a unit test.
    """

    def __init__(
        self,
        settings: KafkaPublisherConfig,
        *,
        producer: _ProducerClient | None = None,
    ) -> None:
        self.settings = settings
        self._producer = producer or Producer(settings.as_confluent_config())

    def publish_order_created(
        self,
        topic: str,
        event: OrderCreatedEvent,
    ) -> KafkaPublishReceipt:
        """Publish one event and wait for Kafka's delivery report.

        Args:
            topic: Destination Kafka topic.
            event: Typed order event to validate and publish.

        Returns:
            Topic, partition, offset, key, timestamp, and headers acknowledged.

        Raises:
            ValueError: If ``topic`` is blank.
            EventPublishError: If Kafka rejects or does not acknowledge the event.
        """
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
            raise EventPublishError(
                f"Kafka rejected event {event.event_id} for topic {topic!r}: {exc}"
            ) from exc

        if undelivered:
            raise EventPublishError(
                f"Kafka did not acknowledge {undelivered} queued record(s) within "
                f"{self.settings.delivery_timeout_seconds:.1f} seconds; "
                f"event_id={event.event_id}, topic={topic!r}."
            )
        if delivery_errors:
            raise EventPublishError(
                f"Kafka delivery failed for event {event.event_id} on topic "
                f"{topic!r}: {delivery_errors[0]}"
            )
        if len(delivered_message) != 1:
            raise EventPublishError(
                f"Kafka flush completed without one delivery report for event "
                f"{event.event_id} on topic {topic!r}."
            )

        message = delivered_message[0]
        timestamp_ms = _message_timestamp_ms(message)
        return KafkaPublishReceipt(
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


@dataclass(frozen=True)
class SqsPublishReceipt:
    """SQS acknowledgement returned after `SendMessage` succeeds."""

    message_id: str
    md5_of_body: str
    sequence_number: str | None = None


class SqsEventPublisher(EventPublisher[SqsPublishReceipt]):
    """Publish validated order events through a boto3-compatible SQS client.

    Args:
        client: Boto3-compatible SQS client.
    """

    def __init__(self, client: Any) -> None:
        self._client = client

    def publish_order_created(
        self,
        destination: str,
        event: OrderCreatedEvent,
        *,
        message_group_id: str | None = None,
        deduplication_id: str | None = None,
    ) -> SqsPublishReceipt:
        """Send one event to an SQS standard or FIFO queue.

        Args:
            destination: Destination queue URL.
            event: Typed order event to validate and send.
            message_group_id: Required ordering group when using a FIFO queue.
            deduplication_id: Optional FIFO deduplication identity; defaults to
                the event ID when a message group is provided.

        Returns:
            SQS message ID, body checksum, and optional FIFO sequence number.

        Raises:
            ValueError: If ``destination`` is blank.
            EventPublishError: If SQS rejects the message.
        """
        if not destination.strip():
            raise ValueError("SQS queue URL must not be blank.")
        validate_order_created_contract(event)
        request: dict[str, Any] = {
            "QueueUrl": destination,
            "MessageBody": json.dumps(
                event_to_wire_dict(event),
                sort_keys=True,
                separators=(",", ":"),
            ),
            "MessageAttributes": {
                "event-type": {
                    "DataType": "String",
                    "StringValue": event.event_type,
                },
                "event-version": {
                    "DataType": "Number",
                    "StringValue": str(event.event_version),
                },
                "event-id": {
                    "DataType": "String",
                    "StringValue": str(event.event_id),
                },
                "correlation-id": {
                    "DataType": "String",
                    "StringValue": event.correlation_id,
                },
                "content-type": {
                    "DataType": "String",
                    "StringValue": "application/json",
                },
            },
        }
        if message_group_id is not None:
            request["MessageGroupId"] = message_group_id
            request["MessageDeduplicationId"] = deduplication_id or str(
                event.event_id
            )
        try:
            response = self._client.send_message(**request)
        except (BotoCoreError, ClientError, OSError) as exc:
            raise EventPublishError(
                f"SQS rejected event {event.event_id} for queue {destination!r}: {exc}"
            ) from exc
        return SqsPublishReceipt(
            message_id=response["MessageId"],
            md5_of_body=response["MD5OfMessageBody"],
            sequence_number=response.get("SequenceNumber"),
        )
