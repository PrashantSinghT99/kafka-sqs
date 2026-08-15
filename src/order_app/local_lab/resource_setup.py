"""Idempotent provisioning for the persistent visual lab."""

from __future__ import annotations

import json
from typing import Any

import boto3

from order_app.messaging import KafkaTopicAdmin, TopicSpec
from order_app.local_lab.consumer_control_store import ConsumerControlStore
from order_app.local_lab.config import LocalLabConfig
from order_app.order_processing import PostgresOrderStore


LOCAL_TOPIC_CONFIG = {
    "cleanup.policy": "delete",
    "retention.ms": "604800000",
}


def build_sqs_client(settings: LocalLabConfig) -> Any:
    """Create a boto3 SQS client for the configured local endpoint.

    Args:
        settings: Configuration containing the SQS endpoint and region.

    Returns:
        A boto3 SQS client configured for LocalStack.
    """
    return boto3.client(
        "sqs",
        endpoint_url=settings.sqs_endpoint_url,
        region_name=settings.sqs_region,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


def ensure_topic(admin: KafkaTopicAdmin, name: str) -> None:
    """Create a three-partition local Kafka topic only when it is absent.

    Args:
        admin: Kafka topic-administration adapter.
        name: Topic name to inspect and possibly create.

    Returns:
        None. Existing topics are left unchanged.
    """
    if name not in admin.list_topic_names():
        admin.create_topic(
            TopicSpec(
                name=name,
                partition_count=3,
                replication_factor=1,
                config=LOCAL_TOPIC_CONFIG,
            )
        )


def ensure_sqs_queues(client: Any, settings: LocalLabConfig) -> tuple[str, str]:
    """Create or retrieve the local order queue and its dead-letter queue.

    Args:
        client: Boto3-compatible SQS client.
        settings: Configuration containing both queue names.

    Returns:
        A ``(queue_url, dead_letter_queue_url)`` tuple.
    """
    dlq_url = client.create_queue(
        QueueName=settings.sqs_dlq_name,
        Attributes={
            "VisibilityTimeout": "10",
            "ReceiveMessageWaitTimeSeconds": "2",
        },
    )["QueueUrl"]
    dlq_arn = client.get_queue_attributes(
        QueueUrl=dlq_url,
        AttributeNames=["QueueArn"],
    )["Attributes"]["QueueArn"]
    queue_url = client.create_queue(
        QueueName=settings.sqs_queue_name,
        Attributes={
            "VisibilityTimeout": "10",
            "ReceiveMessageWaitTimeSeconds": "2",
            "RedrivePolicy": json.dumps(
                {
                    "deadLetterTargetArn": dlq_arn,
                    "maxReceiveCount": "3",
                }
            ),
        },
    )["QueueUrl"]
    return queue_url, dlq_url


def get_queue_url(client: Any, queue_name: str) -> str:
    """Resolve an SQS queue name to its URL.

    Args:
        client: Boto3-compatible SQS client.
        queue_name: Existing queue name.

    Returns:
        The queue URL returned by SQS.
    """
    return client.get_queue_url(QueueName=queue_name)["QueueUrl"]


def initialize_lab(settings: LocalLabConfig) -> None:
    """Provision all persistent Kafka, SQS, and PostgreSQL lab resources.

    Args:
        settings: Complete local-lab configuration.

    Returns:
        None. Resources are created idempotently in their external services.
    """
    admin = KafkaTopicAdmin(settings.kafka_bootstrap_servers, timeout_seconds=20)
    ensure_topic(admin, settings.kafka_topic)
    ensure_topic(admin, settings.kafka_dlq_topic)

    sqs = build_sqs_client(settings)
    queue_url, dlq_url = ensure_sqs_queues(sqs, settings)

    for schema in (settings.kafka_schema, settings.sqs_schema):
        PostgresOrderStore(settings.postgres_dsn, schema=schema).initialize()
    ConsumerControlStore(settings.postgres_dsn).initialize()

    print(
        "Local lab initialized: "
        f"topic={settings.kafka_topic}, kafka_dlq={settings.kafka_dlq_topic}, "
        f"queue={queue_url}, sqs_dlq={dlq_url}, "
        f"schemas={settings.kafka_schema},{settings.sqs_schema}",
        flush=True,
    )
