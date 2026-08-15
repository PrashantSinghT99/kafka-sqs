"""Boto3 SQS publisher and owned-queue observation probe."""

from __future__ import annotations

from dataclasses import dataclass
import json
from time import monotonic
from typing import Any

from order_app.messaging.contracts import (
    OrderCreatedEvent,
    parse_order_created_event,
    validate_order_created_contract,
)
from order_app.messaging.contracts.models import event_to_wire_dict


@dataclass(frozen=True)
class SentSqsMessage:
    message_id: str
    md5_of_body: str
    sequence_number: str | None = None


@dataclass(frozen=True)
class ReceivedSqsEvent:
    message_id: str
    receipt_handle: str
    event: OrderCreatedEvent
    attributes: dict[str, str]
    message_attributes: dict[str, dict[str, str]]


class SqsProbeTimeout(TimeoutError):
    pass


class SqsEventClient:
    """Publish typed events and consume only from test-owned queues."""

    def __init__(self, client: Any) -> None:
        self.client = client

    def publish_order_created(
        self,
        queue_url: str,
        event: OrderCreatedEvent,
        *,
        message_group_id: str | None = None,
        deduplication_id: str | None = None,
    ) -> SentSqsMessage:
        validate_order_created_contract(event)
        request: dict[str, Any] = {
            "QueueUrl": queue_url,
            "MessageBody": json.dumps(
                event_to_wire_dict(event),
                sort_keys=True,
                separators=(",", ":"),
            ),
            "MessageAttributes": {
                "event-type": {"DataType": "String", "StringValue": event.event_type},
                "event-version": {
                    "DataType": "Number",
                    "StringValue": str(event.event_version),
                },
                "event-id": {"DataType": "String", "StringValue": str(event.event_id)},
                "correlation-id": {
                    "DataType": "String",
                    "StringValue": event.correlation_id,
                },
                "content-type": {
                    "DataType": "String",
                    "StringValue": "application/json",
                },
            },
        }
        if message_group_id is not None:
            request["MessageGroupId"] = message_group_id
            request["MessageDeduplicationId"] = deduplication_id or str(event.event_id)
        response = self.client.send_message(**request)
        return SentSqsMessage(
            message_id=response["MessageId"],
            md5_of_body=response["MD5OfMessageBody"],
            sequence_number=response.get("SequenceNumber"),
        )

    def wait_for_event(
        self,
        queue_url: str,
        *,
        correlation_id: str,
        timeout_seconds: float = 10.0,
        delete_observed: bool = True,
    ) -> ReceivedSqsEvent:
        """Observe an owned queue with bounded long polling."""
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
