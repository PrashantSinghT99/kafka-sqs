"""Bounded observation of events on a test-owned SQS queue."""

from __future__ import annotations

from dataclasses import dataclass
import json
from time import monotonic
from typing import Any

from order_app.messaging.contracts import OrderCreatedEvent, parse_order_created_event


@dataclass(frozen=True)
class ReceivedSqsEvent:
    message_id: str
    receipt_handle: str
    event: OrderCreatedEvent
    attributes: dict[str, str]
    message_attributes: dict[str, dict[str, str]]


class SqsProbeTimeout(TimeoutError):
    """Raised when no matching event arrives before the bounded deadline."""


class SqsQueueProbe:
    """Find one correlated event without coupling the application publisher to tests."""

    def __init__(self, client: Any) -> None:
        self.client = client

    def wait_for_event(
        self,
        queue_url: str,
        *,
        correlation_id: str,
        timeout_seconds: float = 10.0,
        delete_observed: bool = True,
    ) -> ReceivedSqsEvent:
        deadline = monotonic() + timeout_seconds
        observed: list[str] = []
        while monotonic() < deadline:
            remaining = deadline - monotonic()
            wait_seconds = max(0, min(2, int(remaining)))
            response = self.client.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=wait_seconds,
                AttributeNames=["All"],
                MessageAttributeNames=["All"],
            )
            for message in response.get("Messages", []):
                payload = json.loads(message["Body"])
                event = parse_order_created_event(payload)
                observed.append(str(event.event_id))
                if delete_observed:
                    self.client.delete_message(
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
