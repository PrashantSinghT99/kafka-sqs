"""Independent Kafka consumer probe for producer-side tests."""

from __future__ import annotations

from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
import json
from time import monotonic
from typing import Any, Protocol
from uuid import UUID, uuid4

from confluent_kafka import Consumer, Message
from pydantic import ValidationError

from order_app.messaging.contracts import (
    ContractValidationError,
    OrderCreatedEvent,
    parse_order_created_event,
)


class KafkaProbeError(RuntimeError):
    """Raised when a probe cannot start or Kafka returns a consumer error."""


class KafkaProbeTimeout(TimeoutError):
    """Raised when no matching event is observed before the deadline."""

    def __init__(
        self,
        *,
        topic: str,
        group_id: str,
        timeout_seconds: float,
        observed_count: int,
        observed_summaries: tuple[str, ...],
    ) -> None:
        self.topic = topic
        self.group_id = group_id
        self.timeout_seconds = timeout_seconds
        self.observed_count = observed_count
        self.observed_summaries = observed_summaries
        evidence = "; ".join(observed_summaries) or "none"
        super().__init__(
            f"No matching Kafka event appeared on topic {topic!r} within "
            f"{timeout_seconds:.2f} seconds; group_id={group_id!r}, "
            f"observed_count={observed_count}, observed=[{evidence}]"
        )


@dataclass(frozen=True)
class ProbeSettings:
    """Explicit, non-committing consumer configuration for a test observer."""

    bootstrap_servers: str
    group_id: str = field(default_factory=lambda: f"order-app-test-probe-{uuid4()}")
    client_id: str = "order-app-test-event-probe"
    offset_reset: str = "earliest"
    startup_timeout_seconds: float = 10.0
    poll_interval_seconds: float = 0.2
    diagnostic_record_limit: int = 10

    def as_confluent_config(self) -> dict[str, object]:
        if self.offset_reset not in {"earliest", "latest"}:
            raise ValueError("offset_reset must be 'earliest' or 'latest'.")
        if self.startup_timeout_seconds <= 0 or self.poll_interval_seconds <= 0:
            raise ValueError("Probe timeouts must be greater than zero.")
        if self.diagnostic_record_limit <= 0:
            raise ValueError("diagnostic_record_limit must be greater than zero.")
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
class ObservedKafkaRecord:
    """One broker record plus its parsed event, when contract-valid."""

    topic: str
    partition: int
    offset: int
    timestamp_ms: int | None
    key: bytes | None
    value: bytes | None
    headers: tuple[tuple[str, bytes | None], ...]
    event: OrderCreatedEvent | None
    parse_error: str | None = None

    @property
    def key_text(self) -> str | None:
        return self.key.decode("utf-8", errors="replace") if self.key else None


class _ConsumerClient(Protocol):
    def subscribe(self, topics: list[str]) -> None: ...

    def poll(self, timeout: float = -1) -> Message | None: ...

    def assignment(self) -> list[Any]: ...

    def close(self) -> None: ...


RecordPredicate = Callable[[ObservedKafkaRecord], bool]


def match_order_created_event(
    *,
    event_id: UUID | str | None = None,
    correlation_id: str | None = None,
) -> RecordPredicate:
    """Create a matcher for one event identity and/or one business journey."""
    if event_id is None and correlation_id is None:
        raise ValueError("Provide event_id, correlation_id, or both.")
    expected_event_id = str(event_id) if event_id is not None else None

    def matches(record: ObservedKafkaRecord) -> bool:
        event = record.event
        return event is not None and (
            expected_event_id is None or str(event.event_id) == expected_event_id
        ) and (
            correlation_id is None or event.correlation_id == correlation_id
        )

    return matches


class KafkaEventProbe:
    """Observe a topic through an isolated consumer group without committing."""

    def __init__(
        self,
        settings: ProbeSettings,
        topic: str,
        *,
        consumer: _ConsumerClient | None = None,
    ) -> None:
        if not topic.strip():
            raise ValueError("Kafka topic must not be blank.")
        self.settings = settings
        self.topic = topic
        self._consumer = consumer or Consumer(settings.as_confluent_config())
        self._started = False
        self._closed = False
        self._pending: deque[ObservedKafkaRecord] = deque()

    def __enter__(self) -> KafkaEventProbe:
        try:
            return self.start()
        except Exception:
            self.close()
            raise

    def __exit__(self, *_: object) -> None:
        self.close()

    def start(self) -> KafkaEventProbe:
        """Subscribe and wait for assignment so this observer precedes the trigger."""
        if self._closed:
            raise KafkaProbeError("A closed Kafka probe cannot be restarted.")
        if self._started:
            return self

        self._consumer.subscribe([self.topic])
        deadline = monotonic() + self.settings.startup_timeout_seconds
        while True:
            remaining = deadline - monotonic()
            if remaining <= 0:
                break
            message = self._consumer.poll(
                min(self.settings.poll_interval_seconds, remaining)
            )
            if message is not None:
                self._pending.append(self._to_record(message))
            if self._consumer.assignment():
                self._started = True
                return self

        raise KafkaProbeError(
            f"Kafka probe did not receive a partition assignment for topic "
            f"{self.topic!r} within {self.settings.startup_timeout_seconds:.2f} "
            f"seconds; group_id={self.settings.group_id!r}."
        )

    def wait_for_event(
        self,
        predicate: RecordPredicate,
        *,
        timeout_seconds: float = 10.0,
    ) -> ObservedKafkaRecord:
        """Return the first matching record, or fail with bounded evidence."""
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be greater than zero.")
        if not self._started:
            self.start()

        deadline = monotonic() + timeout_seconds
        observed_count = 0
        summaries: deque[str] = deque(
            maxlen=self.settings.diagnostic_record_limit
        )

        while True:
            record = self._pending.popleft() if self._pending else None
            if record is None:
                remaining = deadline - monotonic()
                if remaining <= 0:
                    break
                message = self._consumer.poll(
                    min(self.settings.poll_interval_seconds, remaining)
                )
                if message is None:
                    continue
                record = self._to_record(message)

            observed_count += 1
            if predicate(record):
                return record
            summaries.append(_summarize(record))

        raise KafkaProbeTimeout(
            topic=self.topic,
            group_id=self.settings.group_id,
            timeout_seconds=timeout_seconds,
            observed_count=observed_count,
            observed_summaries=tuple(summaries),
        )

    def close(self) -> None:
        """Leave the isolated test group; safe to call more than once."""
        if not self._closed:
            self._consumer.close()
            self._closed = True

    @staticmethod
    def _to_record(message: Message) -> ObservedKafkaRecord:
        error = message.error()
        if error is not None:
            raise KafkaProbeError(
                f"Kafka consumer error at {message.topic()}["
                f"{message.partition()}]: {error}"
            )

        value = message.value()
        event: OrderCreatedEvent | None = None
        parse_error: str | None = None
        try:
            decoded = json.loads(value.decode("utf-8") if value is not None else "")
            if not isinstance(decoded, dict):
                raise TypeError("event JSON must be an object")
            event = parse_order_created_event(decoded)
        except (
            ContractValidationError,
            UnicodeDecodeError,
            json.JSONDecodeError,
            TypeError,
            ValidationError,
            ValueError,
        ) as exc:
            parse_error = f"{type(exc).__name__}: {exc}"

        _, timestamp_ms = message.timestamp()
        return ObservedKafkaRecord(
            topic=message.topic(),
            partition=message.partition(),
            offset=message.offset(),
            timestamp_ms=(
                timestamp_ms
                if timestamp_ms is not None and timestamp_ms >= 0
                else None
            ),
            key=message.key(),
            value=value,
            headers=tuple(message.headers() or ()),
            event=event,
            parse_error=parse_error,
        )


def _summarize(record: ObservedKafkaRecord) -> str:
    location = f"{record.topic}[{record.partition}]@{record.offset}"
    if record.event is not None:
        return (
            f"{location} key={record.key_text!r} "
            f"event_id={record.event.event_id} "
            f"correlation_id={record.event.correlation_id!r}"
        )
    detail = (record.parse_error or "unparseable record").replace("\n", " ")
    return f"{location} key={record.key_text!r} parse_error={detail[:160]!r}"
