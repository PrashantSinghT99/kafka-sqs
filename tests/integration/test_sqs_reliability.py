"""SQS-specific visibility, redrive, FIFO ordering, and dedup tests."""

from uuid import uuid4

import pytest

from tests.helpers.client_stub import eventually
from order_app.messaging.contracts import make_order_created_event
from order_app.messaging import SqsEventPublisher
from tests.helpers.sqs import SqsQueueSet


@pytest.mark.integration
@pytest.mark.sqs
@pytest.mark.reliability
def test_receive_without_delete_becomes_visible_with_higher_receive_count(
    sqs_client,
    sqs_queues: SqsQueueSet,
) -> None:
    event = make_order_created_event(order_id="ORD-SQS-VISIBILITY-1")
    SqsEventPublisher(sqs_client).publish_order_created(sqs_queues.standard_url, event)

    first = _receive_one(sqs_client, sqs_queues.standard_url, wait=1)
    assert first is not None
    assert first["Attributes"]["ApproximateReceiveCount"] == "1"

    hidden = _receive_one(sqs_client, sqs_queues.standard_url, wait=0)
    assert hidden is None

    redelivered = eventually(
        lambda: _receive_one(sqs_client, sqs_queues.standard_url, wait=1),
        lambda value: value is not None,
        timeout_seconds=5,
        interval_seconds=0.1,
        description="SQS message after visibility timeout",
    )
    assert redelivered is not None
    assert int(redelivered["Attributes"]["ApproximateReceiveCount"]) >= 2
    sqs_client.delete_message(
        QueueUrl=sqs_queues.standard_url,
        ReceiptHandle=redelivered["ReceiptHandle"],
    )


@pytest.mark.integration
@pytest.mark.sqs
@pytest.mark.reliability
def test_poison_message_redrives_to_owned_dlq(
    sqs_client,
    sqs_queues: SqsQueueSet,
) -> None:
    event = make_order_created_event(order_id="ORD-SQS-POISON-1")
    SqsEventPublisher(sqs_client).publish_order_created(sqs_queues.standard_url, event)
    counts: list[int] = []

    for _ in range(4):
        message = _receive_one(sqs_client, sqs_queues.standard_url, wait=1)
        if message is None:
            break
        counts.append(int(message["Attributes"]["ApproximateReceiveCount"]))
        sqs_client.change_message_visibility(
            QueueUrl=sqs_queues.standard_url,
            ReceiptHandle=message["ReceiptHandle"],
            VisibilityTimeout=0,
        )

    dead_letter = eventually(
        lambda: _receive_one(sqs_client, sqs_queues.dlq_url, wait=1),
        lambda value: value is not None,
        timeout_seconds=5,
        interval_seconds=0.1,
        description="poison message in SQS DLQ",
    )

    assert counts[:3] == [1, 2, 3]
    assert dead_letter is not None
    assert str(event.event_id) in dead_letter["Body"]
    sqs_client.delete_message(
        QueueUrl=sqs_queues.dlq_url,
        ReceiptHandle=dead_letter["ReceiptHandle"],
    )


@pytest.mark.integration
@pytest.mark.sqs
@pytest.mark.reliability
def test_fifo_preserves_order_within_group_and_deduplicates_id(
    sqs_client,
    sqs_queues: SqsQueueSet,
) -> None:
    event_client = SqsEventPublisher(sqs_client)
    events = [
        make_order_created_event(order_id=f"ORD-SQS-FIFO-{index}")
        for index in range(3)
    ]
    for event in events:
        event_client.publish_order_created(
            sqs_queues.fifo_url,
            event,
            message_group_id="orders-customer-1",
            deduplication_id=str(event.event_id),
        )

    duplicate = make_order_created_event(order_id="ORD-SQS-DEDUPE")
    dedupe_id = f"dedupe-{uuid4()}"
    event_client.publish_order_created(
        sqs_queues.fifo_url,
        duplicate,
        message_group_id="orders-customer-1",
        deduplication_id=dedupe_id,
    )
    event_client.publish_order_created(
        sqs_queues.fifo_url,
        duplicate,
        message_group_id="orders-customer-1",
        deduplication_id=dedupe_id,
    )

    received = sqs_client.receive_message(
        QueueUrl=sqs_queues.fifo_url,
        MaxNumberOfMessages=10,
        WaitTimeSeconds=2,
        AttributeNames=["All"],
    ).get("Messages", [])
    bodies = [message["Body"] for message in received]
    order_positions = [
        next(index for index, body in enumerate(bodies) if str(event.event_id) in body)
        for event in events
    ]

    assert order_positions == sorted(order_positions)
    assert sum(str(duplicate.event_id) in body for body in bodies) == 1
    assert all(
        message["Attributes"]["MessageGroupId"] == "orders-customer-1"
        for message in received
    )
    for message in received:
        sqs_client.delete_message(
            QueueUrl=sqs_queues.fifo_url,
            ReceiptHandle=message["ReceiptHandle"],
        )


def _receive_one(client, queue_url: str, *, wait: int):
    messages = client.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=wait,
        AttributeNames=["All"],
        MessageAttributeNames=["All"],
    ).get("Messages", [])
    return messages[0] if messages else None
