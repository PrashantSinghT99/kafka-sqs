"""Kafka-specific test infrastructure and client utilities."""

from mqtest.kafka.admin import (
    DEFAULT_TEST_TOPIC_PARTITIONS,
    KafkaAdminError,
    KafkaTestAdmin,
    TopicMetadata,
    TopicSpec,
)
from mqtest.kafka.names import unique_topic_name
from mqtest.kafka.settings import KAFKA_IMAGE

__all__ = [
    "DEFAULT_TEST_TOPIC_PARTITIONS",
    "KAFKA_IMAGE",
    "KafkaAdminError",
    "KafkaTestAdmin",
    "TopicMetadata",
    "TopicSpec",
    "unique_topic_name",
]
