"""Kafka-specific test infrastructure and client utilities."""

from mqtest.kafka.admin import (
    DEFAULT_TEST_TOPIC_PARTITIONS,
    KafkaAdminError,
    KafkaTestAdmin,
    TopicMetadata,
    TopicSpec,
)
from mqtest.kafka.names import unique_topic_name
from mqtest.kafka.producer import (
    KafkaEventProducer,
    KafkaPublishError,
    ProducerSettings,
    PublishedRecord,
    order_created_headers,
    serialize_order_created_event,
)
from mqtest.kafka.probe import (
    KafkaEventProbe,
    KafkaProbeError,
    KafkaProbeTimeout,
    ObservedKafkaRecord,
    ProbeSettings,
    match_order_created_event,
)
from mqtest.kafka.settings import KAFKA_IMAGE

__all__ = [
    "DEFAULT_TEST_TOPIC_PARTITIONS",
    "KAFKA_IMAGE",
    "KafkaAdminError",
    "KafkaEventProducer",
    "KafkaEventProbe",
    "KafkaPublishError",
    "KafkaProbeError",
    "KafkaProbeTimeout",
    "KafkaTestAdmin",
    "ObservedKafkaRecord",
    "ProbeSettings",
    "ProducerSettings",
    "PublishedRecord",
    "TopicMetadata",
    "TopicSpec",
    "match_order_created_event",
    "order_created_headers",
    "serialize_order_created_event",
    "unique_topic_name",
]
