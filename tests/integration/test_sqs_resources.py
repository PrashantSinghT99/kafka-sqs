"""Disposable LocalStack and isolated SQS resource tests."""

import json

import pytest

from mqtest.sqs import SqsQueueSet


@pytest.mark.integration
@pytest.mark.sqs
def test_isolated_sqs_queues_have_explicit_attributes(
    sqs_client,
    sqs_queues: SqsQueueSet,
) -> None:
    standard = sqs_queues.standard_attributes
    fifo = sqs_queues.fifo_attributes

    assert standard["VisibilityTimeout"] == "2"
    assert standard["ReceiveMessageWaitTimeSeconds"] == "1"
    redrive = json.loads(standard["RedrivePolicy"])
    assert redrive["maxReceiveCount"] == "3"
    assert redrive["deadLetterTargetArn"] == sqs_queues.dlq_attributes["QueueArn"]
    assert fifo["FifoQueue"] == "true"
    assert fifo["ContentBasedDeduplication"] == "false"
    assert fifo["VisibilityTimeout"] == "2"

    listed = sqs_client.list_queues().get("QueueUrls", [])
    assert sqs_queues.standard_url in listed
    assert sqs_queues.fifo_url in listed
    assert sqs_queues.dlq_url in listed
