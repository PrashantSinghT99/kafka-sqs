"""HTTP-to-Kafka producer component tests with no business consumer."""

from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from mqtest.contracts import validate_order_created_contract
from mqtest.kafka import (
    KafkaEventProbe,
    KafkaEventProducer,
    KafkaProbeTimeout,
    ProbeSettings,
    ProducerSettings,
    TopicMetadata,
    match_order_created_event,
)
from sample_app.order_api import create_order_app


@pytest.mark.integration
@pytest.mark.kafka
def test_post_order_publishes_expected_contract_valid_kafka_record(
    kafka_bootstrap_servers: str,
    kafka_topic: TopicMetadata,
    request: pytest.FixtureRequest,
) -> None:
    correlation_id = f"component-{uuid4()}"
    publisher = KafkaEventProducer(ProducerSettings(kafka_bootstrap_servers))
    app = create_order_app(
        publisher,
        kafka_topic.name,
        order_id_factory=lambda: "ORD-COMPONENT-1",
        causation_id_factory=lambda: "request-component-1",
    )
    probe_settings = ProbeSettings(kafka_bootstrap_servers)

    with KafkaEventProbe(probe_settings, kafka_topic.name) as probe:
        with TestClient(app) as client:
            response = client.post(
                "/orders",
                headers={"X-Correlation-ID": correlation_id},
                json={
                    "customer_id": "CUS-COMPONENT-1",
                    "amount": 749.5,
                    "currency": "INR",
                },
            )
        observed = probe.wait_for_event(
            match_order_created_event(correlation_id=correlation_id),
            timeout_seconds=10,
        )

    assert response.status_code == 202
    event = observed.event
    assert event is not None
    validate_order_created_contract(event)
    assert response.json() == {
        "order_id": "ORD-COMPONENT-1",
        "correlation_id": correlation_id,
        "event_id": str(event.event_id),
    }
    assert response.headers["X-Correlation-ID"] == correlation_id
    assert observed.topic == kafka_topic.name
    assert observed.key_text == "ORD-COMPONENT-1"
    assert event.event_type == "order.created"
    assert event.event_version == 1
    assert event.correlation_id == correlation_id
    assert event.causation_id == "request-component-1"
    assert event.data.model_dump() == {
        "order_id": "ORD-COMPONENT-1",
        "customer_id": "CUS-COMPONENT-1",
        "amount": 749.5,
        "currency": "INR",
    }
    headers = dict(observed.headers)
    assert headers["content-type"] == b"application/json"
    assert headers["event-type"] == b"order.created"
    assert headers["event-version"] == b"1"
    assert headers["event-id"] == str(event.event_id).encode("ascii")
    assert headers["correlation-id"] == correlation_id.encode("utf-8")

    request.node.user_properties.extend(
        [
            ("probe_group_id", probe_settings.group_id),
            ("event_id", str(event.event_id)),
            ("correlation_id", correlation_id),
            ("kafka_topic", observed.topic),
            ("kafka_partition", observed.partition),
            ("kafka_offset", observed.offset),
        ]
    )


@pytest.mark.integration
@pytest.mark.kafka
def test_invalid_post_order_produces_no_matching_kafka_event(
    kafka_bootstrap_servers: str,
    kafka_topic: TopicMetadata,
) -> None:
    correlation_id = f"invalid-component-{uuid4()}"
    publisher = KafkaEventProducer(ProducerSettings(kafka_bootstrap_servers))
    app = create_order_app(publisher, kafka_topic.name)
    probe_settings = ProbeSettings(kafka_bootstrap_servers)

    with KafkaEventProbe(probe_settings, kafka_topic.name) as probe:
        with TestClient(app) as client:
            response = client.post(
                "/orders",
                headers={"X-Correlation-ID": correlation_id},
                json={
                    "customer_id": "CUS-INVALID",
                    "amount": -1.0,
                    "currency": "inr",
                },
            )
        with pytest.raises(KafkaProbeTimeout) as raised:
            probe.wait_for_event(
                match_order_created_event(correlation_id=correlation_id),
                timeout_seconds=1,
            )

    assert response.status_code == 422
    assert raised.value.observed_count == 0
