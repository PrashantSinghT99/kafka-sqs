"""Unit tests for isolated Kafka topic naming."""

import re

import pytest

from order_app.messaging.kafka.names import MAX_TOPIC_NAME_LENGTH, unique_topic_name


@pytest.mark.unit
def test_topic_name_is_readable_and_kafka_safe() -> None:
    name = unique_topic_name(
        "tests/integration/test orders.py::creates order [INR]",
        token="abc123",
    )

    assert name == "order-app-test-tests-integration-test-orders.py-creates-order-inr-abc123"
    assert re.fullmatch(r"[a-z0-9._-]+", name)


@pytest.mark.unit
def test_topic_name_is_unique_by_default() -> None:
    first = unique_topic_name("same-test")
    second = unique_topic_name("same-test")

    assert first != second


@pytest.mark.unit
def test_topic_name_respects_kafka_length_limit() -> None:
    name = unique_topic_name("x" * 400, token="fixedtoken")

    assert len(name) == MAX_TOPIC_NAME_LENGTH
    assert name.endswith("-fixedtoken")
