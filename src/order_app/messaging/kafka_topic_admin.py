"""Kafka topic creation, inspection, and deletion adapter."""

from __future__ import annotations

from dataclasses import dataclass, field
from time import monotonic, sleep
from typing import Mapping

from confluent_kafka.admin import AdminClient, NewTopic


DEFAULT_TOPIC_PARTITIONS = 3
DEFAULT_TOPIC_REPLICATION_FACTOR = 1
DEFAULT_TOPIC_CONFIG: Mapping[str, str] = {
    "cleanup.policy": "delete",
    "retention.ms": "600000",
}


class KafkaAdminError(RuntimeError):
    """Raised when Kafka topic administration fails."""


@dataclass(frozen=True)
class TopicSpec:
    """Desired Kafka topic configuration."""

    name: str
    partition_count: int = DEFAULT_TOPIC_PARTITIONS
    replication_factor: int = DEFAULT_TOPIC_REPLICATION_FACTOR
    config: Mapping[str, str] = field(
        default_factory=lambda: dict(DEFAULT_TOPIC_CONFIG)
    )


@dataclass(frozen=True)
class TopicMetadata:
    """Observed broker metadata plus the configuration requested by the test."""

    name: str
    partition_ids: tuple[int, ...]
    replication_factors: tuple[int, ...]
    requested_config: Mapping[str, str]

    @property
    def partition_count(self) -> int:
        """Read the number of partitions reported by Kafka.

        Returns:
            Number of partition IDs in this metadata value.
        """
        return len(self.partition_ids)


class KafkaTopicAdmin:
    """Create, inspect, and delete Kafka topics with bounded waits.

    Args:
        bootstrap_servers: Comma-separated Kafka broker addresses.
        timeout_seconds: Maximum wait for each administration operation.
        admin_client: Optional compatible client supplied by a unit test.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        *,
        timeout_seconds: float = 10.0,
        admin_client: AdminClient | None = None,
    ) -> None:
        self._timeout_seconds = timeout_seconds
        self._client = admin_client or AdminClient(
            {
                "bootstrap.servers": bootstrap_servers,
                "client.id": "order-app-topic-admin",
                "socket.timeout.ms": int(timeout_seconds * 1_000),
            }
        )

    def create_topic(self, spec: TopicSpec) -> TopicMetadata:
        """Create a topic and wait until Kafka exposes its metadata.

        Args:
            spec: Topic name, partitions, replication, and configuration.

        Returns:
            Metadata observed after successful creation.

        Raises:
            KafkaAdminError: If creation fails or metadata does not stabilize.
        """
        topic = NewTopic(
            spec.name,
            num_partitions=spec.partition_count,
            replication_factor=spec.replication_factor,
            config=dict(spec.config),
        )
        future = self._client.create_topics(
            [topic],
            operation_timeout=self._timeout_seconds,
            request_timeout=self._timeout_seconds,
        )[spec.name]

        try:
            future.result(timeout=self._timeout_seconds)
        except Exception as exc:
            raise KafkaAdminError(
                f"Failed to create Kafka topic {spec.name!r}: {exc}"
            ) from exc

        deadline = monotonic() + self._timeout_seconds
        last_metadata_error: KafkaAdminError | None = None
        while monotonic() < deadline:
            try:
                metadata = self.describe_topic(
                    spec.name,
                    requested_config=spec.config,
                )
            except KafkaAdminError as exc:
                last_metadata_error = exc
            else:
                if metadata.partition_count == spec.partition_count:
                    return metadata
            sleep(0.1)

        detail = f" Last metadata result: {last_metadata_error}" if last_metadata_error else ""
        raise KafkaAdminError(
            f"Kafka topic {spec.name!r} did not expose {spec.partition_count} "
            f"partitions within {self._timeout_seconds:.1f} seconds.{detail}"
        )

    def describe_topic(
        self,
        name: str,
        *,
        requested_config: Mapping[str, str] | None = None,
    ) -> TopicMetadata:
        """Read partition and replica metadata for an existing topic.

        Args:
            name: Existing Kafka topic name.
            requested_config: Optional desired settings retained in the result.

        Returns:
            Current topic metadata from Kafka.

        Raises:
            KafkaAdminError: If the topic is missing or unavailable.
        """
        cluster_metadata = self._client.list_topics(timeout=self._timeout_seconds)
        topic_metadata = cluster_metadata.topics.get(name)
        if topic_metadata is None or topic_metadata.error is not None:
            detail = topic_metadata.error if topic_metadata is not None else "not found"
            raise KafkaAdminError(f"Kafka topic {name!r} is unavailable: {detail}")

        partition_ids = tuple(sorted(topic_metadata.partitions))
        replication_factors = tuple(
            len(topic_metadata.partitions[partition_id].replicas)
            for partition_id in partition_ids
        )
        return TopicMetadata(
            name=name,
            partition_ids=partition_ids,
            replication_factors=replication_factors,
            requested_config=dict(requested_config or {}),
        )

    def list_topic_names(self) -> frozenset[str]:
        """List the topic names currently reported by Kafka.

        Returns:
            An immutable set of topic names.
        """
        metadata = self._client.list_topics(timeout=self._timeout_seconds)
        return frozenset(metadata.topics)

    def delete_topic(self, name: str) -> None:
        """Delete a topic and wait until Kafka no longer reports it.

        Args:
            name: Existing Kafka topic name.

        Returns:
            None after the topic disappears from metadata.

        Raises:
            KafkaAdminError: If deletion fails or the topic remains visible.
        """
        future = self._client.delete_topics(
            [name],
            operation_timeout=self._timeout_seconds,
            request_timeout=self._timeout_seconds,
        )[name]
        try:
            future.result(timeout=self._timeout_seconds)
        except Exception as exc:
            raise KafkaAdminError(
                f"Failed to delete Kafka topic {name!r}: {exc}"
            ) from exc

        deadline = monotonic() + self._timeout_seconds
        while monotonic() < deadline:
            if name not in self.list_topic_names():
                return
            sleep(0.1)

        raise KafkaAdminError(
            f"Kafka topic {name!r} remained visible for more than "
            f"{self._timeout_seconds:.1f} seconds after deletion."
        )
