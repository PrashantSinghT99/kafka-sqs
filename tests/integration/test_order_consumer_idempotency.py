"""Idempotency tests for duplicate delivery and correlation reuse."""

from uuid import uuid4

import pytest

from order_app.messaging.contracts import make_order_created_event
from tests.helpers.http import RecordingHttpStub
from order_app.messaging.kafka import KafkaEventProducer, ProducerSettings, TopicMetadata
from order_app.order_consumer import (
    ConsumerSettings,
    KafkaOrderConsumer,
    OrderConsumerError,
    OrderNotificationClient,
    PostgresOrderStore,
)


@pytest.mark.integration
@pytest.mark.kafka
@pytest.mark.reliability
def test_same_event_delivered_twice_creates_one_business_and_http_effect(
    kafka_bootstrap_servers: str,
    kafka_topic: TopicMetadata,
    order_store: PostgresOrderStore,
) -> None:
    event = make_order_created_event(order_id="ORD-IDEMPOTENT-1")
    producer = KafkaEventProducer(ProducerSettings(kafka_bootstrap_servers))
    producer.publish_order_created(kafka_topic.name, event)
    producer.publish_order_created(kafka_topic.name, event)
    group_id = f"idempotent-consumer-{uuid4()}"

    with RecordingHttpStub() as stub:
        stub.enqueue_response(202)
        downstream = OrderNotificationClient(stub.base_url)
        with KafkaOrderConsumer(
            ConsumerSettings(kafka_bootstrap_servers, group_id),
            kafka_topic.name,
            order_store,
            downstream=downstream,
        ) as consumer:
            first = consumer.process_one(timeout_seconds=10)
            second = consumer.process_one(timeout_seconds=10)

    assert first.duplicate is False
    assert second.duplicate is True
    assert order_store.order_count() == 1
    assert order_store.processed_event_count() == 1
    assert order_store.has_processed(event.event_id) is True
    assert len(stub.requests) == 1


@pytest.mark.integration
@pytest.mark.kafka
@pytest.mark.reliability
def test_different_events_with_same_correlation_id_are_both_processed(
    kafka_bootstrap_servers: str,
    kafka_topic: TopicMetadata,
    order_store: PostgresOrderStore,
) -> None:
    correlation_id = f"shared-journey-{uuid4()}"
    first_event = make_order_created_event(
        order_id="ORD-CORRELATION-1",
        correlation_id=correlation_id,
    )
    second_event = make_order_created_event(
        order_id="ORD-CORRELATION-2",
        correlation_id=correlation_id,
    )
    producer = KafkaEventProducer(ProducerSettings(kafka_bootstrap_servers))
    producer.publish_order_created(kafka_topic.name, first_event)
    producer.publish_order_created(kafka_topic.name, second_event)
    group_id = f"correlation-consumer-{uuid4()}"

    with RecordingHttpStub() as stub:
        stub.enqueue_response(202)
        stub.enqueue_response(202)
        downstream = OrderNotificationClient(stub.base_url)
        with KafkaOrderConsumer(
            ConsumerSettings(kafka_bootstrap_servers, group_id),
            kafka_topic.name,
            order_store,
            downstream=downstream,
        ) as consumer:
            first = consumer.process_one(timeout_seconds=10)
            second = consumer.process_one(timeout_seconds=10)

    assert first.duplicate is False
    assert second.duplicate is False
    assert order_store.order_count() == 2
    assert order_store.processed_event_count() == 2
    assert order_store.has_processed(first_event.event_id) is True
    assert order_store.has_processed(second_event.event_id) is True
    assert len(stub.requests) == 2
    assert {
        request.headers["x-correlation-id"] for request in stub.requests
    } == {correlation_id}


@pytest.mark.integration
@pytest.mark.kafka
@pytest.mark.reliability
def test_pending_redelivery_retries_unfinished_downstream_effect(
    kafka_bootstrap_servers: str,
    kafka_topic: TopicMetadata,
    order_store: PostgresOrderStore,
) -> None:
    event = make_order_created_event(order_id="ORD-PENDING-RETRY-1")
    producer = KafkaEventProducer(ProducerSettings(kafka_bootstrap_servers))
    producer.publish_order_created(kafka_topic.name, event)
    group_id = f"pending-consumer-{uuid4()}"
    settings = ConsumerSettings(kafka_bootstrap_servers, group_id)

    with RecordingHttpStub() as stub:
        stub.enqueue_response(503)
        downstream = OrderNotificationClient(stub.base_url, max_attempts=1)
        with KafkaOrderConsumer(
            settings,
            kafka_topic.name,
            order_store,
            downstream=downstream,
        ) as consumer:
            with pytest.raises(OrderConsumerError, match="before offset commit"):
                consumer.process_one(timeout_seconds=10)

        assert order_store.order_count() == 1
        assert order_store.has_processed(event.event_id) is False

        stub.enqueue_response(202)
        with KafkaOrderConsumer(
            settings,
            kafka_topic.name,
            order_store,
            downstream=downstream,
        ) as restarted:
            processed = restarted.process_one(timeout_seconds=10)

    assert processed.duplicate is True
    assert order_store.order_count() == 1
    assert order_store.processed_event_count() == 1
    assert order_store.has_processed(event.event_id) is True
    assert len(stub.requests) == 2
