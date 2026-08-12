"""Deterministic consumer retry and dead-letter reliability tests."""

import json
from uuid import uuid4

import pytest

from mqtest.contracts import make_order_created_event
from mqtest.http import RecordingHttpStub
from mqtest.kafka import (
    KafkaEventProbe,
    KafkaEventProducer,
    ProbeSettings,
    ProducerSettings,
    TopicMetadata,
)
from sample_app.order_consumer import (
    ConsumerSettings,
    DownstreamNotificationError,
    KafkaDeadLetterPublisher,
    KafkaOrderConsumer,
    OrderNotificationClient,
    PostgresOrderStore,
    RetryPolicy,
)


@pytest.mark.integration
@pytest.mark.kafka
@pytest.mark.reliability
def test_transient_downstream_failure_recovers_on_configured_attempt(
    kafka_bootstrap_servers: str,
    kafka_topic: TopicMetadata,
    order_store: PostgresOrderStore,
) -> None:
    event = make_order_created_event(order_id="ORD-RETRY-POLICY-1")
    KafkaEventProducer(ProducerSettings(kafka_bootstrap_servers)).publish_order_created(
        kafka_topic.name,
        event,
    )
    retry_policy = RetryPolicy(
        max_attempts=3,
        backoff_seconds=0,
        retryable_errors=(DownstreamNotificationError,),
    )

    with RecordingHttpStub() as stub:
        stub.enqueue_response(503)
        stub.enqueue_response(503)
        stub.enqueue_response(202)
        downstream = OrderNotificationClient(stub.base_url, max_attempts=1)
        with KafkaOrderConsumer(
            ConsumerSettings(
                kafka_bootstrap_servers,
                f"retry-policy-{uuid4()}",
            ),
            kafka_topic.name,
            order_store,
            downstream=downstream,
            retry_policy=retry_policy,
        ) as consumer:
            processed = consumer.process_one(timeout_seconds=10)

    assert processed.attempts == 3
    assert processed.dead_lettered is False
    assert len(stub.requests) == 3
    assert order_store.order_count() == 1
    assert order_store.has_processed(event.event_id) is True


@pytest.mark.integration
@pytest.mark.kafka
@pytest.mark.reliability
def test_exhausted_poison_event_reaches_dlq_and_later_event_continues(
    kafka_bootstrap_servers: str,
    kafka_topic: TopicMetadata,
    kafka_dlq_topic: TopicMetadata,
    order_store: PostgresOrderStore,
) -> None:
    correlation_id = f"poison-journey-{uuid4()}"
    poison = make_order_created_event(
        order_id="ORD-POISON-1",
        correlation_id=correlation_id,
    )
    later_valid = make_order_created_event(
        order_id="ORD-POISON-1",
        correlation_id=correlation_id,
    )
    producer = KafkaEventProducer(ProducerSettings(kafka_bootstrap_servers))
    producer.publish_order_created(kafka_topic.name, poison)
    producer.publish_order_created(kafka_topic.name, later_valid)
    retry_policy = RetryPolicy(
        max_attempts=3,
        backoff_seconds=0,
        retryable_errors=(DownstreamNotificationError,),
    )
    dlq_publisher = KafkaDeadLetterPublisher(kafka_bootstrap_servers)
    probe_settings = ProbeSettings(kafka_bootstrap_servers)

    with RecordingHttpStub() as stub:
        stub.enqueue_response(503)
        stub.enqueue_response(503)
        stub.enqueue_response(503)
        stub.enqueue_response(202)
        downstream = OrderNotificationClient(stub.base_url, max_attempts=1)

        with KafkaEventProbe(probe_settings, kafka_dlq_topic.name) as dlq_probe:
            with KafkaOrderConsumer(
                ConsumerSettings(
                    kafka_bootstrap_servers,
                    f"dlq-policy-{uuid4()}",
                ),
                kafka_topic.name,
                order_store,
                downstream=downstream,
                retry_policy=retry_policy,
                dead_letter_publisher=dlq_publisher,
                dead_letter_topic=kafka_dlq_topic.name,
            ) as consumer:
                failed = consumer.process_one(timeout_seconds=10)
                continued = consumer.process_one(timeout_seconds=10)

            observed_dlq = dlq_probe.wait_for_event(
                lambda record: _dlq_event_id(record.value) == str(poison.event_id),
                timeout_seconds=10,
            )

    assert failed.dead_lettered is True
    assert failed.attempts == 3
    assert continued.dead_lettered is False
    assert continued.event_id == str(later_valid.event_id)
    assert len(stub.requests) == 4
    assert order_store.order_count() == 1
    assert order_store.processed_event_count() == 1
    stored = order_store.fetch_order("ORD-POISON-1")
    assert stored is not None
    assert stored.source_event_id == later_valid.event_id
    assert order_store.has_processed(poison.event_id) is False
    assert order_store.has_processed(later_valid.event_id) is True

    assert observed_dlq.value is not None
    dead_letter = json.loads(observed_dlq.value)
    assert dead_letter["source_topic"] == kafka_topic.name
    assert dead_letter["source_partition"] == failed.partition
    assert dead_letter["source_offset"] == failed.offset
    assert dead_letter["event_id"] == str(poison.event_id)
    assert dead_letter["correlation_id"] == correlation_id
    assert dead_letter["attempts"] == 3
    assert dead_letter["error_type"] == "DownstreamNotificationError"
    assert dead_letter["original_payload"]["event_id"] == str(poison.event_id)


def _dlq_event_id(value: bytes | None) -> str | None:
    if value is None:
        return None
    return json.loads(value)["event_id"]
