"""Unit tests for deterministic Kafka topic administration."""

from __future__ import annotations

from typing import Any, cast

from confluent_kafka.admin import AdminClient
import pytest

from mqtest.kafka.admin import (
    KafkaAdminError,
    KafkaTestAdmin,
    TopicMetadata,
    TopicSpec,
)


class _CompletedFuture:
    def result(self, timeout: float) -> None:
        assert timeout > 0


class _CreateOnlyAdminClient:
    def create_topics(self, topics: list[Any], **_: Any) -> dict[str, _CompletedFuture]:
        return {topics[0].topic: _CompletedFuture()}


@pytest.mark.unit
def test_create_topic_waits_for_metadata_propagation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    admin = KafkaTestAdmin(
        "unused:9092",
        timeout_seconds=1.0,
        admin_client=cast(AdminClient, _CreateOnlyAdminClient()),
    )
    spec = TopicSpec(name="mqtest-eventual-metadata")
    expected = TopicMetadata(
        name=spec.name,
        partition_ids=(0, 1, 2),
        replication_factors=(1, 1, 1),
        requested_config=dict(spec.config),
    )
    attempts = 0

    def describe_after_propagation(
        name: str,
        *,
        requested_config: Any = None,
    ) -> TopicMetadata:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise KafkaAdminError(f"Kafka topic {name!r} is unavailable: not found")
        assert requested_config == spec.config
        return expected

    monkeypatch.setattr(admin, "describe_topic", describe_after_propagation)

    created = admin.create_topic(spec)

    assert created == expected
    assert attempts == 2

