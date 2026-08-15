"""Real Kafka-to-PostgreSQL consumer component tests."""

from decimal import Decimal
from uuid import uuid4

import pytest

from order_app.messaging.contracts import make_order_created_event
from order_app.messaging import KafkaEventPublisher, KafkaPublisherConfig, TopicMetadata
from order_app.order_processing import (
    ConsumerSettings,
    KafkaOrderConsumer,
    OrderConsumerError,
    PostgresOrderStore,
)


@pytest.mark.integration
@pytest.mark.kafka
def test_sdk_event_creates_order_and_processed_identity_atomically(
    kafka_bootstrap_servers: str,
    kafka_topic: TopicMetadata,
    order_store: PostgresOrderStore,
    request: pytest.FixtureRequest,
) -> None:
    event = make_order_created_event(
        order_id="ORD-CONSUMER-1",
        customer_id="CUS-CONSUMER-1",
        amount=499.25,
        currency="INR",
    )
    KafkaEventPublisher(KafkaPublisherConfig(kafka_bootstrap_servers)).publish_order_created(
        kafka_topic.name,
        event,
    )
    group_id = f"order-consumer-{uuid4()}"

    with KafkaOrderConsumer(
        ConsumerSettings(kafka_bootstrap_servers, group_id),
        kafka_topic.name,
        order_store,
    ) as consumer:
        processed = consumer.process_one(timeout_seconds=10)

    order = order_store.fetch_order(event.data.order_id)
    assert order is not None
    assert order.order_id == "ORD-CONSUMER-1"
    assert order.customer_id == "CUS-CONSUMER-1"
    assert order.amount == Decimal("499.25")
    assert order.currency.strip() == "INR"
    assert order.source_event_id == event.event_id
    assert order.correlation_id == event.correlation_id
    assert order_store.has_processed(event.event_id) is True
    assert processed.event_id == str(event.event_id)

    request.node.user_properties.extend(
        [
            ("consumer_group_id", group_id),
            ("event_id", str(event.event_id)),
            ("correlation_id", event.correlation_id),
            ("kafka_partition", processed.partition),
            ("kafka_offset", processed.offset),
            ("postgres_schema", order_store.schema),
        ]
    )


@pytest.mark.integration
@pytest.mark.kafka
def test_database_rollback_leaves_offset_uncommitted_for_restart(
    kafka_bootstrap_servers: str,
    kafka_topic: TopicMetadata,
    order_store: PostgresOrderStore,
) -> None:
    event = make_order_created_event(order_id="ORD-ROLLBACK-1")
    KafkaEventPublisher(KafkaPublisherConfig(kafka_bootstrap_servers)).publish_order_created(
        kafka_topic.name,
        event,
    )
    group_id = f"rollback-consumer-{uuid4()}"
    settings = ConsumerSettings(kafka_bootstrap_servers, group_id)
    order_store.drop_processed_events_table()

    with KafkaOrderConsumer(settings, kafka_topic.name, order_store) as consumer:
        with pytest.raises(OrderConsumerError, match="before offset commit"):
            consumer.process_one(timeout_seconds=10)

    assert order_store.fetch_order(event.data.order_id) is None
    order_store.initialize()

    with KafkaOrderConsumer(settings, kafka_topic.name, order_store) as restarted:
        processed = restarted.process_one(timeout_seconds=10)

    assert processed.event_id == str(event.event_id)
    assert order_store.fetch_order(event.data.order_id) is not None
    assert order_store.has_processed(event.event_id) is True
