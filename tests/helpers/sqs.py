"""SQS-only test support: queue lifecycle and independent event observation."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import re
from time import monotonic
from typing import Any
from uuid import uuid4

from order_app.messaging.contracts import OrderCreatedEvent, parse_order_created_event


_UNSAFE = re.compile(r"[^a-z0-9_-]+")


def unique_queue_name(seed: str, *, fifo: bool = False) -> str:
    """Build a unique, readable SQS queue name within AWS limits.

    Args:
        seed: Usually the pytest node ID that identifies the owning test.
        fifo: Append the required ``.fifo`` suffix when ``True``.

    Returns:
        An AWS-safe queue name no longer than 80 characters.
    """
    readable = _UNSAFE.sub("-", seed.lower()).strip("-_") or "queue"
    suffix = uuid4().hex[:10]
    digest = hashlib.sha1(seed.encode("utf-8")).hexdigest()[:8]
    ending = ".fifo" if fifo else ""
    fixed = f"order-app-test--{digest}-{suffix}"
    readable_limit = 80 - len(ending) - len(fixed)
    safe_readable = readable[:readable_limit].rstrip("-_")
    base = f"order-app-test-{safe_readable}-{digest}-{suffix}"
    return f"{base}{ending}"


@dataclass(frozen=True)
class SqsQueueSet:
    """URLs and observable attributes for one isolated queue family."""

    standard_url: str
    fifo_url: str
    dlq_url: str
    standard_attributes: dict[str, str]
    fifo_attributes: dict[str, str]
    dlq_attributes: dict[str, str]


def create_sqs_queue_set(client: Any, seed: str) -> SqsQueueSet:
    """Provision an isolated standard, FIFO, and dead-letter queue family.

    Args:
        client: Boto3-compatible SQS client.
        seed: Test identity used to create unique names.

    Returns:
        Queue URLs and their observable SQS attributes.
    """
    dlq_url = _create_sqs_queue(
        client,
        unique_queue_name(f"{seed}-dlq"),
        {"VisibilityTimeout": "2", "ReceiveMessageWaitTimeSeconds": "1"},
    )
    dlq_attributes = get_sqs_queue_attributes(client, dlq_url)
    standard_url = _create_sqs_queue(
        client,
        unique_queue_name(f"{seed}-standard"),
        {
            "VisibilityTimeout": "2",
            "ReceiveMessageWaitTimeSeconds": "1",
            "RedrivePolicy": json.dumps(
                {
                    "deadLetterTargetArn": dlq_attributes["QueueArn"],
                    "maxReceiveCount": "3",
                }
            ),
        },
    )
    fifo_url = _create_sqs_queue(
        client,
        unique_queue_name(f"{seed}-fifo", fifo=True),
        {
            "FifoQueue": "true",
            "ContentBasedDeduplication": "false",
            "VisibilityTimeout": "2",
            "ReceiveMessageWaitTimeSeconds": "1",
        },
    )
    return SqsQueueSet(
        standard_url=standard_url,
        fifo_url=fifo_url,
        dlq_url=dlq_url,
        standard_attributes=get_sqs_queue_attributes(client, standard_url),
        fifo_attributes=get_sqs_queue_attributes(client, fifo_url),
        dlq_attributes=dlq_attributes,
    )


def delete_sqs_queue_set(client: Any, queues: SqsQueueSet) -> None:
    """Delete every queue in an isolated test queue family.

    Args:
        client: Boto3-compatible SQS client.
        queues: Queue URLs previously returned by ``create_sqs_queue_set``.

    Returns:
        None after all three delete requests are accepted.
    """
    for queue_url in (queues.standard_url, queues.fifo_url, queues.dlq_url):
        client.delete_queue(QueueUrl=queue_url)


def get_sqs_queue_attributes(client: Any, queue_url: str) -> dict[str, str]:
    """Read all observable attributes for one SQS queue.

    Args:
        client: Boto3-compatible SQS client.
        queue_url: Existing queue URL.

    Returns:
        Mapping of SQS attribute names to string values.
    """
    return client.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=["All"],
    )["Attributes"]


def _create_sqs_queue(
    client: Any,
    name: str,
    attributes: dict[str, str],
) -> str:
    return client.create_queue(QueueName=name, Attributes=attributes)["QueueUrl"]


@dataclass(frozen=True)
class ReceivedSqsEvent:
    """Hold a parsed SQS event and the broker evidence used by assertions."""
    message_id: str
    receipt_handle: str
    event: OrderCreatedEvent
    attributes: dict[str, str]
    message_attributes: dict[str, dict[str, str]]


class SqsProbeTimeout(TimeoutError):
    """Raised when no matching event arrives before the bounded deadline."""


def wait_for_sqs_event(
    client: Any,
    queue_url: str,
    *,
    correlation_id: str,
    timeout_seconds: float = 10.0,
    delete_observed: bool = True,
) -> ReceivedSqsEvent:
    """Poll a test-owned queue until a correlated order event appears.

    Args:
        client: Boto3-compatible SQS client.
        queue_url: Test-owned queue URL.
        correlation_id: Business journey identity to match.
        timeout_seconds: Overall wait limit.
        delete_observed: Delete received messages after inspecting them.

    Returns:
        The matching typed event plus SQS receipt and attribute evidence.

    Raises:
        SqsProbeTimeout: If no matching event arrives before the deadline.
    """
    deadline = monotonic() + timeout_seconds
    observed: list[str] = []
    while monotonic() < deadline:
        remaining = deadline - monotonic()
        wait_seconds = max(0, min(2, int(remaining)))
        response = client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=wait_seconds,
            AttributeNames=["All"],
            MessageAttributeNames=["All"],
        )
        for message in response.get("Messages", []):
            event = parse_order_created_event(json.loads(message["Body"]))
            observed.append(str(event.event_id))
            if delete_observed:
                client.delete_message(
                    QueueUrl=queue_url,
                    ReceiptHandle=message["ReceiptHandle"],
                )
            if event.correlation_id == correlation_id:
                return ReceivedSqsEvent(
                    message_id=message["MessageId"],
                    receipt_handle=message["ReceiptHandle"],
                    event=event,
                    attributes=message.get("Attributes", {}),
                    message_attributes=message.get("MessageAttributes", {}),
                )
    raise SqsProbeTimeout(
        f"No matching SQS event within {timeout_seconds:.2f} seconds; "
        f"queue_url={queue_url!r}, correlation_id={correlation_id!r}, "
        f"observed_event_ids={observed!r}."
    )
