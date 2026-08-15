"""Real downstream HTTP interaction tests for the Kafka consumer."""

from uuid import uuid4

import pytest

from tests.helpers.eventually import eventually
from order_app.messaging.contracts import make_order_created_event
from tests.helpers.http import RecordingHttpStub
from order_app.messaging.kafka import KafkaEventProducer, ProducerSettings, TopicMetadata
from order_app.order_consumer import (
    ConsumerSettings,
    KafkaOrderConsumer,
    OrderNotificationClient,
    PostgresOrderStore,
)


@pytest.mark.integration
@pytest.mark.kafka
def test_consumer_sends_expected_downstream_http_request(
    kafka_bootstrap_servers: str,
    kafka_topic: TopicMetadata,
    order_store: PostgresOrderStore,
    request: pytest.FixtureRequest,
) -> None:
    event = make_order_created_event(
        order_id="ORD-HTTP-1",
        customer_id="CUS-HTTP-1",
        amount=320.5,
        currency="EUR",
    )
    group_id = f"http-consumer-{uuid4()}"

    with RecordingHttpStub() as stub:
        stub.enqueue_response(202, json_body={"accepted": True})
        downstream = OrderNotificationClient(stub.base_url)
        KafkaEventProducer(
            ProducerSettings(kafka_bootstrap_servers)
        ).publish_order_created(kafka_topic.name, event)

        with KafkaOrderConsumer(
            ConsumerSettings(kafka_bootstrap_servers, group_id),
            kafka_topic.name,
            order_store,
            downstream=downstream,
        ) as consumer:
            processed = consumer.process_one(timeout_seconds=10)

        observed = eventually(
            lambda: stub.requests[0] if stub.requests else None,
            lambda value: value is not None,
            timeout_seconds=2,
            interval_seconds=0.01,
            description="downstream order-created request",
        )

    assert observed is not None
    assert observed.method == "POST"
    assert observed.path == "/order-created"
    assert observed.headers["content-type"] == "application/json"
    assert observed.headers["x-correlation-id"] == event.correlation_id
    assert observed.headers["x-event-id"] == str(event.event_id)
    assert observed.json() == {
        "order_id": "ORD-HTTP-1",
        "customer_id": "CUS-HTTP-1",
        "amount": 320.5,
        "currency": "EUR",
    }
    assert processed.event_id == str(event.event_id)
    assert order_store.fetch_order(event.data.order_id) is not None

    request.node.user_properties.extend(
        [
            ("consumer_group_id", group_id),
            ("event_id", str(event.event_id)),
            ("correlation_id", event.correlation_id),
            ("downstream_path", observed.path),
        ]
    )


@pytest.mark.integration
@pytest.mark.kafka
def test_temporary_downstream_failure_retries_then_commits(
    kafka_bootstrap_servers: str,
    kafka_topic: TopicMetadata,
    order_store: PostgresOrderStore,
) -> None:
    event = make_order_created_event(order_id="ORD-HTTP-RETRY-1")
    group_id = f"http-retry-consumer-{uuid4()}"

    with RecordingHttpStub() as stub:
        stub.enqueue_response(503, json_body={"error": "temporary"})
        stub.enqueue_response(202, json_body={"accepted": True})
        downstream = OrderNotificationClient(stub.base_url, max_attempts=2)
        KafkaEventProducer(
            ProducerSettings(kafka_bootstrap_servers)
        ).publish_order_created(kafka_topic.name, event)

        with KafkaOrderConsumer(
            ConsumerSettings(kafka_bootstrap_servers, group_id),
            kafka_topic.name,
            order_store,
            downstream=downstream,
        ) as consumer:
            processed = consumer.process_one(timeout_seconds=10)

    assert len(stub.requests) == 2
    assert all(
        recorded.headers["x-correlation-id"] == event.correlation_id
        for recorded in stub.requests
    )
    assert processed.event_id == str(event.event_id)
    assert order_store.has_processed(event.event_id) is True
