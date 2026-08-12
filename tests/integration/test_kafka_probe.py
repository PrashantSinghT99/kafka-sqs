"""Real Kafka tests for the independent producer-test probe."""

import pytest

from mqtest.contracts import make_order_created_event
from mqtest.kafka import (
    KafkaEventProbe,
    KafkaEventProducer,
    KafkaProbeTimeout,
    ProbeSettings,
    ProducerSettings,
    TopicMetadata,
    match_order_created_event,
)


@pytest.mark.integration
@pytest.mark.kafka
def test_probe_started_before_trigger_finds_only_the_correlated_event(
    kafka_bootstrap_servers: str,
    kafka_topic: TopicMetadata,
    request: pytest.FixtureRequest,
) -> None:
    unrelated = make_order_created_event(correlation_id="checkout-unrelated")
    expected = make_order_created_event(correlation_id="checkout-probe-target")
    producer = KafkaEventProducer(ProducerSettings(kafka_bootstrap_servers))
    settings = ProbeSettings(kafka_bootstrap_servers)

    with KafkaEventProbe(settings, kafka_topic.name) as probe:
        producer.publish_order_created(kafka_topic.name, unrelated)
        published = producer.publish_order_created(kafka_topic.name, expected)
        observed = probe.wait_for_event(
            match_order_created_event(correlation_id=expected.correlation_id),
            timeout_seconds=10,
        )

    request.node.user_properties.extend(
        [
            ("probe_group_id", settings.group_id),
            ("event_id", str(expected.event_id)),
            ("correlation_id", expected.correlation_id),
            ("kafka_partition", observed.partition),
            ("kafka_offset", observed.offset),
        ]
    )
    assert observed.event == expected
    assert observed.topic == kafka_topic.name
    assert observed.key_text == expected.data.order_id
    assert observed.partition == published.partition
    assert observed.offset == published.offset


@pytest.mark.integration
@pytest.mark.kafka
def test_probe_missing_event_fails_at_bounded_deadline_with_evidence(
    kafka_bootstrap_servers: str,
    kafka_topic: TopicMetadata,
) -> None:
    producer = KafkaEventProducer(ProducerSettings(kafka_bootstrap_servers))
    unrelated = make_order_created_event(correlation_id="checkout-observed")
    settings = ProbeSettings(kafka_bootstrap_servers)

    with KafkaEventProbe(settings, kafka_topic.name) as probe:
        producer.publish_order_created(kafka_topic.name, unrelated)
        with pytest.raises(KafkaProbeTimeout) as raised:
            probe.wait_for_event(
                match_order_created_event(correlation_id="checkout-missing"),
                timeout_seconds=1,
            )

    error = raised.value
    assert error.group_id == settings.group_id
    assert error.observed_count == 1
    assert str(unrelated.event_id) in str(error)
