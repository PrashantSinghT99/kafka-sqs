"""Kafka topic isolation and AdminClient lifecycle tests."""

import pytest

from order_app.messaging.kafka import KafkaTestAdmin, TopicMetadata, TopicSpec, unique_topic_name


@pytest.mark.integration
@pytest.mark.kafka
def test_isolated_topic_exposes_expected_metadata(
    kafka_topic: TopicMetadata,
) -> None:
    assert kafka_topic.partition_ids == (0, 1, 2)
    assert kafka_topic.replication_factors == (1, 1, 1)
    assert kafka_topic.requested_config == {
        "cleanup.policy": "delete",
        "retention.ms": "600000",
    }


@pytest.mark.integration
@pytest.mark.kafka
def test_admin_topic_lifecycle_removes_deleted_topic(
    kafka_admin: KafkaTestAdmin,
    request: pytest.FixtureRequest,
) -> None:
    topic_name = unique_topic_name(request.node.nodeid)
    created = kafka_admin.create_topic(TopicSpec(name=topic_name))

    try:
        observed = kafka_admin.describe_topic(topic_name)
        assert observed.partition_ids == created.partition_ids
        assert topic_name in kafka_admin.list_topic_names()
    finally:
        kafka_admin.delete_topic(topic_name)

    assert topic_name not in kafka_admin.list_topic_names()

