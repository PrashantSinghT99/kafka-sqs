"""Asynchronous SDK-to-consumer-to-database component test."""

from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from uuid import uuid4

import pytest

from tests.helpers.eventually import eventually
from order_app.messaging.contracts import make_order_created_event
from order_app.messaging.kafka import KafkaEventProducer, ProducerSettings, TopicMetadata
from order_app.order_consumer import (
    ConsumerSettings,
    KafkaOrderConsumer,
    PostgresOrderStore,
)


@pytest.mark.integration
@pytest.mark.kafka
def test_controlled_event_eventually_creates_complete_business_state(
    kafka_bootstrap_servers: str,
    kafka_topic: TopicMetadata,
    order_store: PostgresOrderStore,
    request: pytest.FixtureRequest,
) -> None:
    event = make_order_created_event(
        order_id="ORD-EVENTUALLY-1",
        customer_id="CUS-EVENTUALLY-1",
        amount=875.75,
        currency="USD",
    )
    group_id = f"eventually-consumer-{uuid4()}"

    with KafkaOrderConsumer(
        ConsumerSettings(kafka_bootstrap_servers, group_id),
        kafka_topic.name,
        order_store,
    ) as consumer:
        with ThreadPoolExecutor(max_workers=1) as executor:
            processing = executor.submit(consumer.process_one, timeout_seconds=10)
            KafkaEventProducer(
                ProducerSettings(kafka_bootstrap_servers)
            ).publish_order_created(kafka_topic.name, event)

            order = eventually(
                lambda: order_store.fetch_order(event.data.order_id),
                lambda observed: observed is not None,
                timeout_seconds=10,
                interval_seconds=0.05,
                description=f"PostgreSQL order {event.data.order_id}",
            )
            processed = processing.result(timeout=10)

    assert order is not None
    assert order.order_id == event.data.order_id
    assert order.customer_id == event.data.customer_id
    assert order.amount == Decimal("875.75")
    assert order.currency.strip() == "USD"
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
