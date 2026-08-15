"""SQS adapter for the sample order consumer business store."""

from __future__ import annotations

import json
from typing import Any

from order_app.messaging.contracts import parse_order_created_event
from order_app.order_consumer.store import PostgresOrderStore


class SqsOrderConsumer:
    def __init__(self, client: Any, queue_url: str, store: PostgresOrderStore) -> None:
        self.client = client
        self.queue_url = queue_url
        self.store = store

    def process_one(self, *, wait_seconds: int = 2) -> str:
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
