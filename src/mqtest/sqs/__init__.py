"""SQS-specific clients and isolated resource helpers."""

from mqtest.sqs.resources import (
    SqsQueueSet,
    SqsTestResources,
    unique_queue_name,
)
from mqtest.sqs.settings import LOCALSTACK_IMAGE

__all__ = [
    "LOCALSTACK_IMAGE",
    "SqsQueueSet",
    "SqsTestResources",
    "unique_queue_name",
]
