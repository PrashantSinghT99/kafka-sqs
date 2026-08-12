"""Kafka resource naming for isolated and parallel-safe tests."""

from __future__ import annotations

import re
from uuid import uuid4

MAX_TOPIC_NAME_LENGTH = 249
_INVALID_TOPIC_CHARACTERS = re.compile(r"[^a-z0-9._-]+")
_REPEATED_SEPARATORS = re.compile(r"[-_.]{2,}")


def unique_topic_name(
    test_name: str,
    *,
    prefix: str = "mqtest",
    token: str | None = None,
) -> str:
    """Return a readable Kafka topic name with a collision-resistant suffix."""
    unique_token = token or uuid4().hex[:12]
    base = _normalize_topic_segment(f"{prefix}-{test_name}") or "mqtest-test"
    normalized_token = _normalize_topic_segment(unique_token) or uuid4().hex[:12]
    suffix = f"-{normalized_token}"

    available_base_length = MAX_TOPIC_NAME_LENGTH - len(suffix)
    if available_base_length < 1:
        raise ValueError("The topic-name token is too long for Kafka's 249-character limit.")

    truncated_base = base[:available_base_length].rstrip("-_.") or "t"
    return f"{truncated_base}{suffix}"


def _normalize_topic_segment(value: str) -> str:
    normalized = _INVALID_TOPIC_CHARACTERS.sub("-", value.lower())
    normalized = _REPEATED_SEPARATORS.sub("-", normalized)
    return normalized.strip("-_.")

