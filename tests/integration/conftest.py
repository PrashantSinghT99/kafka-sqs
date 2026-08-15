"""Disposable infrastructure fixtures for integration tests."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import suppress
from uuid import uuid4

import pytest
from testcontainers.kafka import KafkaContainer
from testcontainers.localstack import LocalStackContainer
from testcontainers.postgres import PostgresContainer

from tests.helpers.infrastructure import POSTGRES_IMAGE
from tests.helpers.infrastructure.docker import DockerUnavailableError, require_docker
from order_app.messaging.kafka import (
    KAFKA_IMAGE,
    KafkaAdminError,
    KafkaTestAdmin,
    TopicMetadata,
    TopicSpec,
    unique_topic_name,
)
from order_app.messaging.sqs import LOCALSTACK_IMAGE, SqsQueueSet, SqsTestResources
from order_app.order_consumer import PostgresOrderStore


@pytest.fixture(scope="session")
def kafka_container() -> Iterator[KafkaContainer]:
    """Start one disposable KRaft Kafka broker for the integration session."""
    try:
        require_docker()
    except DockerUnavailableError as exc:
        pytest.fail(str(exc), pytrace=False)

    container = (
        KafkaContainer(image=KAFKA_IMAGE)
        .with_kraft()
        .with_env("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .with_env("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
    )
    try:
        container.start()
    except Exception as exc:
        with suppress(Exception):
            container.stop()
        pytest.fail(
            f"Docker is reachable, but Kafka image {KAFKA_IMAGE!r} failed to start. "
            f"Inspect the container/runtime error: {exc}",
            pytrace=True,
        )

    try:
        yield container
    finally:
        container.stop()


@pytest.fixture(scope="session")
def kafka_bootstrap_servers(kafka_container: KafkaContainer) -> str:
    """Expose the host-reachable Kafka bootstrap-server address."""
    return kafka_container.get_bootstrap_server()


@pytest.fixture(scope="session")
def kafka_admin(kafka_bootstrap_servers: str) -> KafkaTestAdmin:
    """Expose the reusable topic lifecycle helper for the test session."""
    return KafkaTestAdmin(kafka_bootstrap_servers)


@pytest.fixture
def kafka_topic(
    request: pytest.FixtureRequest,
    kafka_admin: KafkaTestAdmin,
) -> Iterator[TopicMetadata]:
    """Provision one isolated three-partition topic for a single test."""
    spec = TopicSpec(name=unique_topic_name(request.node.nodeid))
    metadata = kafka_admin.create_topic(spec)

    request.node.user_properties.extend(
        [
            ("kafka_topic", metadata.name),
            ("kafka_partitions", metadata.partition_count),
            ("kafka_replication_factors", repr(metadata.replication_factors)),
            ("kafka_requested_config", repr(dict(metadata.requested_config))),
        ]
    )

    try:
        yield metadata
    finally:
        try:
            kafka_admin.delete_topic(metadata.name)
        except KafkaAdminError as exc:
            pytest.fail(f"Failed to clean isolated Kafka topic: {exc}")


@pytest.fixture
def kafka_dlq_topic(
    request: pytest.FixtureRequest,
    kafka_admin: KafkaTestAdmin,
) -> Iterator[TopicMetadata]:
    """Provision a second isolated topic for terminal failure evidence."""
    spec = TopicSpec(name=unique_topic_name(f"{request.node.nodeid}-dlq"))
    metadata = kafka_admin.create_topic(spec)
    try:
        yield metadata
    finally:
        try:
            kafka_admin.delete_topic(metadata.name)
        except KafkaAdminError as exc:
            pytest.fail(f"Failed to clean isolated Kafka DLQ topic: {exc}")


@pytest.fixture(scope="session")
def postgres_container() -> Iterator[PostgresContainer]:
    """Start one disposable PostgreSQL server for consumer integration tests."""
    try:
        require_docker()
    except DockerUnavailableError as exc:
        pytest.fail(str(exc), pytrace=False)

    container = PostgresContainer(image=POSTGRES_IMAGE, driver=None)
    try:
        container.start()
    except Exception as exc:
        with suppress(Exception):
            container.stop()
        pytest.fail(
            f"Docker is reachable, but PostgreSQL image {POSTGRES_IMAGE!r} "
            f"failed to start: {exc}",
            pytrace=True,
        )

    try:
        yield container
    finally:
        container.stop()


@pytest.fixture(scope="session")
def postgres_dsn(postgres_container: PostgresContainer) -> str:
    """Expose a host-reachable DSN without an ORM-specific driver suffix."""
    return postgres_container.get_connection_url(driver=None)


@pytest.fixture
def order_store(postgres_dsn: str) -> Iterator[PostgresOrderStore]:
    """Give each test an isolated PostgreSQL schema and deterministic cleanup."""
    store = PostgresOrderStore(
        postgres_dsn,
        schema=f"test_{uuid4().hex}",
    )
    store.initialize()
    try:
        yield store
    finally:
        store.drop_schema()


@pytest.fixture(scope="session")
def localstack_container() -> Iterator[LocalStackContainer]:
    """Start pinned SQS-compatible LocalStack with no real AWS credentials."""
    try:
        require_docker()
    except DockerUnavailableError as exc:
        pytest.fail(str(exc), pytrace=False)

    container = LocalStackContainer(image=LOCALSTACK_IMAGE).with_services("sqs")
    try:
        container.start()
    except Exception as exc:
        with suppress(Exception):
            container.stop()
        pytest.fail(
            f"LocalStack image {LOCALSTACK_IMAGE!r} failed to start: {exc}",
            pytrace=True,
        )
    try:
        yield container
    finally:
        container.stop()


@pytest.fixture(scope="session")
def sqs_client(localstack_container: LocalStackContainer):
    """Expose boto3 SQS configured only for the disposable local endpoint."""
    return localstack_container.get_client("sqs")


@pytest.fixture
def sqs_queues(
    request: pytest.FixtureRequest,
    sqs_client,
) -> Iterator[SqsQueueSet]:
    """Give one test its own standard, FIFO, and DLQ queue family."""
    resources = SqsTestResources(sqs_client)
    queues = resources.create_queue_set(request.node.nodeid)
    request.node.user_properties.extend(
        [
            ("sqs_standard_url", queues.standard_url),
            ("sqs_fifo_url", queues.fifo_url),
            ("sqs_dlq_url", queues.dlq_url),
        ]
    )
    try:
        yield queues
    finally:
        resources.delete_queue_set(queues)
