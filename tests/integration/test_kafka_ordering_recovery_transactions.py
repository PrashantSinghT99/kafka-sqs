"""Kafka-specific ordering, restart, and transactional visibility tests."""

from uuid import uuid4

from confluent_kafka import Producer
import pytest

from order_app.messaging.contracts import OrderCreatedEvent, make_order_created_event
from order_app.messaging.kafka import (
    KafkaEventProbe,
    KafkaEventProducer,
    KafkaProbeTimeout,
    ProbeSettings,
    ProducerSettings,
    TopicMetadata,
    match_order_created_event,
    order_created_headers,
    serialize_order_created_event,
)
from order_app.order_consumer import (
    ConsumerSettings,
    KafkaOrderConsumer,
    PostgresOrderStore,
)


@pytest.mark.integration
@pytest.mark.kafka
@pytest.mark.reliability
def test_same_key_records_share_partition_and_increase_offset_in_order(
    kafka_bootstrap_servers: str,
    kafka_topic: TopicMetadata,
) -> None:
    events = [
        make_order_created_event(
            order_id="ORD-ORDERED-KEY",
            correlation_id="ordered-journey",
        )
        for _ in range(4)
    ]
    producer = KafkaEventProducer(ProducerSettings(kafka_bootstrap_servers))

    with KafkaEventProbe(
        ProbeSettings(kafka_bootstrap_servers),
        kafka_topic.name,
    ) as probe:
        published = [
            producer.publish_order_created(kafka_topic.name, event)
            for event in events
        ]
        observed = [
            probe.wait_for_event(
                match_order_created_event(event_id=event.event_id),
                timeout_seconds=10,
            )
            for event in events
        ]

    assert len({record.partition for record in published}) == 1
    assert [record.offset for record in published] == sorted(
        record.offset for record in published
    )
    assert [record.event.event_id for record in observed if record.event] == [
        event.event_id for event in events
    ]
    assert [record.offset for record in observed] == [
        record.offset for record in published
    ]


@pytest.mark.integration
@pytest.mark.kafka
@pytest.mark.reliability
def test_different_keys_are_asserted_only_within_their_partitions(
    kafka_bootstrap_servers: str,
    kafka_topic: TopicMetadata,
) -> None:
    producer = KafkaEventProducer(ProducerSettings(kafka_bootstrap_servers))
    published = [
        producer.publish_order_created(
            kafka_topic.name,
            make_order_created_event(order_id=f"ORD-KEY-{index}"),
        )
        for index in range(12)
    ]

    by_partition: dict[int, list[int]] = {}
    for record in published:
        by_partition.setdefault(record.partition, []).append(record.offset)

    assert len(by_partition) >= 2
    assert all(offsets == sorted(offsets) for offsets in by_partition.values())
    # No comparison is made between offsets from different partitions: Kafka
    # provides no topic-wide/global ordering guarantee.


@pytest.mark.integration
@pytest.mark.kafka
@pytest.mark.reliability
def test_restart_with_same_group_resumes_after_committed_record(
    kafka_bootstrap_servers: str,
    kafka_topic: TopicMetadata,
    order_store: PostgresOrderStore,
) -> None:
    producer = KafkaEventProducer(ProducerSettings(kafka_bootstrap_servers))
    first_event = make_order_created_event(order_id="ORD-RESTART-1")
    second_event = make_order_created_event(order_id="ORD-RESTART-2")
    group_id = f"restart-consumer-{uuid4()}"
    settings = ConsumerSettings(kafka_bootstrap_servers, group_id)

    producer.publish_order_created(kafka_topic.name, first_event)
    with KafkaOrderConsumer(settings, kafka_topic.name, order_store) as first:
        first_result = first.process_one(timeout_seconds=10)

    producer.publish_order_created(kafka_topic.name, second_event)
    with KafkaOrderConsumer(settings, kafka_topic.name, order_store) as restarted:
        second_result = restarted.process_one(timeout_seconds=10)

    assert first_result.event_id == str(first_event.event_id)
    assert second_result.event_id == str(second_event.event_id)
    assert order_store.order_count() == 2


@pytest.mark.integration
@pytest.mark.kafka
@pytest.mark.reliability
def test_read_committed_probe_excludes_aborted_transaction(
    kafka_bootstrap_servers: str,
    kafka_topic: TopicMetadata,
) -> None:
    aborted = make_order_created_event(order_id="ORD-TX-ABORTED")
    committed = make_order_created_event(order_id="ORD-TX-COMMITTED")
    producer = Producer(
        {
            "bootstrap.servers": kafka_bootstrap_servers,
            "transactional.id": f"transaction-test-{uuid4()}",
            "enable.idempotence": True,
        }
    )
    producer.init_transactions(30)

    with KafkaEventProbe(
        ProbeSettings(kafka_bootstrap_servers),
        kafka_topic.name,
    ) as probe:
        _transactional_publish(producer, kafka_topic.name, aborted)
        producer.abort_transaction(10)

        _transactional_publish(producer, kafka_topic.name, committed)
        producer.commit_transaction(10)

        visible = probe.wait_for_event(
            match_order_created_event(event_id=committed.event_id),
            timeout_seconds=10,
        )
        with pytest.raises(KafkaProbeTimeout) as hidden:
            probe.wait_for_event(
                match_order_created_event(event_id=aborted.event_id),
                timeout_seconds=1,
            )

    assert visible.event == committed
    assert hidden.value.observed_count == 0


def _transactional_publish(
    producer: Producer,
    topic: str,
    event: OrderCreatedEvent,
) -> None:
    producer.begin_transaction()
    producer.produce(
        topic,
        key=event.data.order_id.encode("utf-8"),
        value=serialize_order_created_event(event),
        headers=list(order_created_headers(event)),
    )
    producer.flush(10)
