"""Function-owned SQS queue provisioning and cleanup."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import re
from typing import Any
from uuid import uuid4


_UNSAFE = re.compile(r"[^a-z0-9_-]+")


def unique_queue_name(seed: str, *, fifo: bool = False) -> str:
    """Build an AWS-safe readable name below the 80-character limit."""
    readable = _UNSAFE.sub("-", seed.lower()).strip("-_") or "queue"
    suffix = uuid4().hex[:10]
    digest = hashlib.sha1(seed.encode("utf-8")).hexdigest()[:8]
    ending = ".fifo" if fifo else ""
    fixed = f"mqtest--{digest}-{suffix}"
    readable_limit = 80 - len(ending) - len(fixed)
    safe_readable = readable[:readable_limit].rstrip("-_")
    base = f"mqtest-{safe_readable}-{digest}-{suffix}"
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


class SqsTestResources:
    """Provision standard/FIFO/DLQ queues through a supplied boto3 client."""

    def __init__(self, client: Any) -> None:
        self.client = client

    def create_queue_set(self, seed: str) -> SqsQueueSet:
        dlq_url = self._create(
            unique_queue_name(f"{seed}-dlq"),
            {
                "VisibilityTimeout": "2",
                "ReceiveMessageWaitTimeSeconds": "1",
            },
        )
        dlq_attributes = self.attributes(dlq_url)
        standard_url = self._create(
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
        fifo_url = self._create(
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
            standard_attributes=self.attributes(standard_url),
            fifo_attributes=self.attributes(fifo_url),
            dlq_attributes=dlq_attributes,
        )

    def delete_queue_set(self, queues: SqsQueueSet) -> None:
        for queue_url in (queues.standard_url, queues.fifo_url, queues.dlq_url):
            self.client.delete_queue(QueueUrl=queue_url)

    def attributes(self, queue_url: str) -> dict[str, str]:
        return self.client.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=["All"],
        )["Attributes"]

    def _create(self, name: str, attributes: dict[str, str]) -> str:
        return self.client.create_queue(
            QueueName=name,
            Attributes=attributes,
        )["QueueUrl"]
