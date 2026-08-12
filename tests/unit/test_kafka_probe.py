"""Unit tests for the independent Kafka producer-test probe."""

from __future__ import annotations

from typing import Any

import pytest

from mqtest.contracts import make_order_created_event
from mqtest.kafka import (
    KafkaEventProbe,
    KafkaProbeTimeout,
    ProbeSettings,
    match_order_created_event,
    serialize_order_created_event,
)


class _FakeMessage:
    def __init__(
        self,
        *,
        value: bytes,
        offset: int,
        key: bytes = b"ORD-100",
    ) -> None:
        self._value = value
        self._offset = offset
        self._key = key

    def error(self) -> None:
        return None

    def topic(self) -> str:
        return "orders"

    def partition(self) -> int:
        return 1

    def offset(self) -> int:
        return self._offset

    def timestamp(self) -> tuple[int, int]:
        return (1, 1_786_515_200_000)

    def key(self) -> bytes:
        return self._key

    def value(self) -> bytes:
        return self._value

    def headers(self) -> list[tuple[str, bytes]]:
        return [("content-type", b"application/json")]


class _FakeConsumer:
    def __init__(self, messages: list[_FakeMessage] | None = None) -> None:
        self.messages = list(messages or [])
        self.subscriptions: list[str] = []
        self.closed = False
        self.poll_count = 0

    def subscribe(self, topics: list[str]) -> None:
        self.subscriptions = topics

    def poll(self, timeout: float = -1) -> Any:
        self.poll_count += 1
        return self.messages.pop(0) if self.messages else None

    def assignment(self) -> list[str]:
        return ["orders[1]"] if self.subscriptions else []

    def close(self) -> None:
        self.closed = True


@pytest.mark.unit
def test_probe_config_uses_unique_non_committing_observer_group() -> None:
    first = ProbeSettings("kafka:9092")
    second = ProbeSettings("kafka:9092")

    config = first.as_confluent_config()

    assert first.group_id != second.group_id
    assert config["group.id"] == first.group_id
    assert config["enable.auto.commit"] is False
    assert config["enable.auto.offset.store"] is False
    assert config["auto.offset.reset"] == "earliest"
    assert config["isolation.level"] == "read_committed"


@pytest.mark.unit
def test_probe_skips_unrelated_record_and_matches_correlation_id() -> None:
    unrelated = make_order_created_event(correlation_id="checkout-other")
    expected = make_order_created_event(correlation_id="checkout-expected")
    fake = _FakeConsumer(
        [
            _FakeMessage(value=serialize_order_created_event(unrelated), offset=4),
            _FakeMessage(value=serialize_order_created_event(expected), offset=5),
        ]
    )

    with KafkaEventProbe(
        ProbeSettings("unused:9092", group_id="probe-test"),
        "orders",
        consumer=fake,
    ) as probe:
        observed = probe.wait_for_event(
            match_order_created_event(correlation_id="checkout-expected"),
            timeout_seconds=0.1,
        )

    assert observed.event == expected
    assert observed.offset == 5
    assert fake.subscriptions == ["orders"]
    assert fake.closed is True


@pytest.mark.unit
def test_probe_skips_malformed_record_before_valid_event() -> None:
    expected = make_order_created_event()
    fake = _FakeConsumer(
        [
            _FakeMessage(value=b"not-json", offset=8),
            _FakeMessage(value=serialize_order_created_event(expected), offset=9),
        ]
    )
    probe = KafkaEventProbe(
        ProbeSettings("unused:9092", group_id="malformed-test"),
        "orders",
        consumer=fake,
    )

    observed = probe.wait_for_event(
        match_order_created_event(event_id=expected.event_id),
        timeout_seconds=0.1,
    )
    probe.close()

    assert observed.event == expected
    assert observed.offset == 9


@pytest.mark.unit
def test_probe_timeout_reports_group_and_compact_observed_evidence() -> None:
    unrelated = make_order_created_event(correlation_id="checkout-other")
    fake = _FakeConsumer(
        [_FakeMessage(value=serialize_order_created_event(unrelated), offset=12)]
    )
    probe = KafkaEventProbe(
        ProbeSettings("unused:9092", group_id="diagnostic-group"),
        "orders",
        consumer=fake,
    )

    with pytest.raises(KafkaProbeTimeout) as raised:
        probe.wait_for_event(
            match_order_created_event(correlation_id="missing"),
            timeout_seconds=0.001,
        )
    probe.close()

    error = raised.value
    assert error.group_id == "diagnostic-group"
    assert error.observed_count == 1
    assert "orders[1]@12" in str(error)
    assert str(unrelated.event_id) in str(error)


@pytest.mark.unit
def test_event_matcher_requires_at_least_one_identity() -> None:
    with pytest.raises(ValueError, match="Provide event_id"):
        match_order_created_event()
