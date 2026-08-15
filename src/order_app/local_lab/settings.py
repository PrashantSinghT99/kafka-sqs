"""Environment-backed settings shared by local-lab processes."""

from __future__ import annotations

from dataclasses import dataclass
import os


@dataclass(frozen=True)
class LocalLabSettings:
    kafka_bootstrap_servers: str = "kafka:9092"
    kafka_topic: str = "orders.created.local"
    kafka_dlq_topic: str = "orders.created.local.dlq"
    kafka_consumer_group: str = "local-order-consumer"
    sqs_endpoint_url: str = "http://localstack:4566"
    sqs_region: str = "us-east-1"
    sqs_queue_name: str = "orders-created-local"
    sqs_dlq_name: str = "orders-created-local-dlq"
    postgres_dsn: str = "postgresql://mqtest:mqtest@postgres:5432/mqtest"
    kafka_schema: str = "kafka_lab"
    sqs_schema: str = "sqs_lab"
    console_url: str = "http://127.0.0.1:8088"
    adminer_url: str = "http://127.0.0.1:8089"

    @classmethod
    def from_environment(cls) -> LocalLabSettings:
        defaults = cls()
        return cls(
            kafka_bootstrap_servers=_value(
                "KAFKA_BOOTSTRAP_SERVERS", defaults.kafka_bootstrap_servers
            ),
            kafka_topic=_value("KAFKA_TOPIC", defaults.kafka_topic),
            kafka_dlq_topic=_value("KAFKA_DLQ_TOPIC", defaults.kafka_dlq_topic),
            kafka_consumer_group=_value(
                "KAFKA_CONSUMER_GROUP", defaults.kafka_consumer_group
            ),
            sqs_endpoint_url=_value(
                "SQS_ENDPOINT_URL", defaults.sqs_endpoint_url
            ),
            sqs_region=_value("AWS_DEFAULT_REGION", defaults.sqs_region),
            sqs_queue_name=_value("SQS_QUEUE_NAME", defaults.sqs_queue_name),
            sqs_dlq_name=_value("SQS_DLQ_NAME", defaults.sqs_dlq_name),
            postgres_dsn=_value("POSTGRES_DSN", defaults.postgres_dsn),
            kafka_schema=_value("KAFKA_SCHEMA", defaults.kafka_schema),
            sqs_schema=_value("SQS_SCHEMA", defaults.sqs_schema),
            console_url=_value("KAFKA_CONSOLE_URL", defaults.console_url),
            adminer_url=_value("ADMINER_URL", defaults.adminer_url),
        )


def _value(name: str, default: str) -> str:
    value = os.getenv(name, default).strip()
    if not value:
        raise RuntimeError(f"Local-lab setting {name} must not be blank.")
    return value
