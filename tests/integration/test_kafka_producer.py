"""Real Kafka delivery acknowledgement tests."""

import pytest

from order_app.messaging.contracts import make_order_created_event
from order_app.messaging.kafka import KafkaEventProducer, ProducerSettings, TopicMetadata


@pytest.mark.integration
@pytest.mark.kafka
def test_order_event_is_acknowledged_by_disposable_kafka(
    kafka_bootstrap_servers: str,
    kafka_topic: TopicMetadata,
    request: pytest.FixtureRequest,
) -> None:
    event = make_order_created_event(order_id="ORD-501")
    producer = KafkaEventProducer(ProducerSettings(kafka_bootstrap_servers))

    published = producer.publish_order_created(kafka_topic.name, event)

    request.node.user_properties.extend(
        [
            ("event_id", str(event.event_id)),
            ("correlation_id", event.correlation_id),
            ("kafka_partition", published.partition),
            ("kafka_offset", published.offset),
        ]
    )
    assert published.topic == kafka_topic.name
    assert published.partition in kafka_topic.partition_ids
    assert published.offset >= 0
    assert published.key == "ORD-501"

