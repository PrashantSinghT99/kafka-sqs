"""Unit tests for SQS resource names and explicit queue configuration."""

import pytest

from order_app.messaging.sqs import unique_queue_name


@pytest.mark.unit
def test_queue_names_are_unique_safe_and_bounded() -> None:
    first = unique_queue_name("tests/example.py::test case with spaces")
    second = unique_queue_name("tests/example.py::test case with spaces")

    assert first != second
    assert len(first) <= 80
    assert all(character.isalnum() or character in "-_" for character in first)


@pytest.mark.unit
def test_fifo_queue_name_has_required_suffix_and_limit() -> None:
    name = unique_queue_name("x" * 200, fifo=True)

    assert name.endswith(".fifo")
    assert len(name) <= 80


@pytest.mark.unit
def test_long_queue_seeds_preserve_role_identity_after_truncation() -> None:
    shared = "long-test-node-" * 20

    standard = unique_queue_name(f"{shared}-standard")
    dlq = unique_queue_name(f"{shared}-dlq")

    assert standard != dlq
