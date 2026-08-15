"""Publish validated order events through a supplied boto3 SQS client."""

from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any

from order_app.messaging.contracts import (
    OrderCreatedEvent,
    validate_order_created_contract,
)
from order_app.messaging.contracts.models import event_to_wire_dict


@dataclass(frozen=True)
class SentSqsMessage:
    message_id: str
    md5_of_body: str
    sequence_number: str | None = None


class SqsEventClient:
    """Publish typed order events to SQS."""

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
