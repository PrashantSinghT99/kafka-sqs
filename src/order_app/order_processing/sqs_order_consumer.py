"""SQS order consumer that persists successful messages before deletion."""

from __future__ import annotations

import json
from typing import Any

from order_app.messaging.contracts import parse_order_created_event
from order_app.order_processing.postgres_order_store import PostgresOrderStore


class SqsOrderConsumer:
    """Process SQS order messages and delete them only after persistence.

    Args:
        client: Boto3-compatible SQS client.
        queue_url: Source order queue URL.
        store: PostgreSQL order persistence adapter.
    """
    def __init__(self, client: Any, queue_url: str, store: PostgresOrderStore) -> None:
        self.client = client
        self.queue_url = queue_url
        self.store = store

    def process_one(self, *, wait_seconds: int = 2) -> str:
        """Receive, validate, persist, and delete one SQS message.

        Args:
            wait_seconds: SQS long-poll duration from 0 through 20 seconds.

        Returns:
            The processed event ID as a string.

        Raises:
            TimeoutError: If the long poll returns no message.
            ContractValidationError: If the message body violates the contract.
        """
        response = self.client.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=wait_seconds,
            AttributeNames=["All"],
            MessageAttributeNames=["All"],
        )
        messages = response.get("Messages", [])
        if not messages:
            raise TimeoutError("No SQS order event arrived before long poll ended.")
        message = messages[0]
        event = parse_order_created_event(json.loads(message["Body"]))
        result = self.store.store(event)
        if result.downstream_required:
            self.store.mark_completed(event.event_id)
        self.client.delete_message(
            QueueUrl=self.queue_url,
            ReceiptHandle=message["ReceiptHandle"],
        )
        return str(event.event_id)
