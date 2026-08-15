"""Bounded retry policy and Kafka dead-letter publication."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from typing import Any, Protocol
from uuid import uuid4

from confluent_kafka import Message, Producer

from order_app.messaging import EventPublishError, KafkaPublishReceipt


@dataclass(frozen=True)
class RetryPolicy:
    """Hold retry attempts, backoff, and retryable exception types."""

    max_attempts: int = 1
    backoff_seconds: float = 0.0
    retryable_errors: tuple[type[Exception], ...] = ()

    def __post_init__(self) -> None:
        if self.max_attempts <= 0:
            raise ValueError("max_attempts must be greater than zero.")
        if self.backoff_seconds < 0:
            raise ValueError("backoff_seconds must not be negative.")

    def is_retryable(self, error: Exception) -> bool:
        """Check whether an exception is eligible for another attempt.

        Args:
            error: Processing exception to classify.

        Returns:
            ``True`` when its type appears in ``retryable_errors``.
        """
        return isinstance(error, self.retryable_errors)


@dataclass(frozen=True)
class DeadLetterFailure:
    """Original record and terminal processing evidence written to the DLQ."""

    source_topic: str
    source_partition: int
    source_offset: int
    key: str | None
    event_id: str | None
    correlation_id: str | None
    attempts: int
    error_type: str
    error_message: str
    original_payload: object

    def to_wire_dict(self) -> dict[str, object]:
        """Convert terminal failure evidence into a JSON-compatible payload.

        Returns:
            A dictionary including a new dead-letter ID and failure timestamp.
        """
        return {
            "dead_letter_id": str(uuid4()),
            "failed_at": datetime.now(timezone.utc).isoformat().replace(
                "+00:00", "Z"
            ),
            "source_topic": self.source_topic,
            "source_partition": self.source_partition,
            "source_offset": self.source_offset,
            "key": self.key,
            "event_id": self.event_id,
            "correlation_id": self.correlation_id,
            "attempts": self.attempts,
            "error_type": self.error_type,
            "error_message": self.error_message,
            "original_payload": self.original_payload,
        }


class _ProducerClient(Protocol):
    def produce(self, topic: str, **kwargs: Any) -> None: ...

    def flush(self, timeout: float = -1) -> int: ...


class KafkaDeadLetterPublisher:
    """Publish terminal consumer failures to Kafka and require acknowledgement.

    Args:
        bootstrap_servers: Comma-separated Kafka broker addresses.
        delivery_timeout_seconds: Maximum acknowledgement wait.
        producer: Optional compatible producer supplied by a unit test.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        *,
        delivery_timeout_seconds: float = 10.0,
        producer: _ProducerClient | None = None,
    ) -> None:
        self.delivery_timeout_seconds = delivery_timeout_seconds
        self._producer = producer or Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "client.id": "order-app-dlq-producer",
                "enable.idempotence": True,
                "acks": "all",
                "delivery.timeout.ms": int(delivery_timeout_seconds * 1_000),
            }
        )

    def publish_failure(
        self,
        topic: str,
        failure: DeadLetterFailure,
    ) -> KafkaPublishReceipt:
        """Publish one terminal processing failure to a DLQ topic.

        Args:
            topic: Destination Kafka dead-letter topic.
            failure: Source record and final error evidence.

        Returns:
            Kafka topic, partition, offset, key, timestamp, and headers.

        Raises:
            EventPublishError: If Kafka rejects or does not acknowledge it.
        """
        wire = failure.to_wire_dict()
        key = failure.event_id or failure.key or str(wire["dead_letter_id"])
        delivered: list[Message] = []
        errors: list[object] = []

        def on_delivery(error: object | None, message: Message) -> None:
            if error is not None:
                errors.append(error)
            else:
                delivered.append(message)

        self._producer.produce(
            topic,
            key=key.encode("utf-8"),
            value=json.dumps(
                wire,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8"),
            headers=[
                ("content-type", b"application/json"),
                ("source-topic", failure.source_topic.encode("utf-8")),
                ("error-type", failure.error_type.encode("utf-8")),
                ("attempts", str(failure.attempts).encode("ascii")),
            ],
            on_delivery=on_delivery,
        )
        remaining = self._producer.flush(self.delivery_timeout_seconds)
        if remaining or errors or len(delivered) != 1:
            detail = errors[0] if errors else f"undelivered={remaining}"
            raise EventPublishError(
                f"DLQ publication failed for event {failure.event_id}: {detail}"
            )

        message = delivered[0]
        _, timestamp_ms = message.timestamp()
        return KafkaPublishReceipt(
            topic=message.topic(),
            partition=message.partition(),
            offset=message.offset(),
            timestamp_ms=(
                timestamp_ms
                if timestamp_ms is not None and timestamp_ms >= 0
                else None
            ),
            key=key,
            headers=(
                ("content-type", b"application/json"),
                ("source-topic", failure.source_topic.encode("utf-8")),
                ("error-type", failure.error_type.encode("utf-8")),
                ("attempts", str(failure.attempts).encode("ascii")),
            ),
        )
