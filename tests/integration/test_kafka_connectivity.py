"""Kafka broker lifecycle and connectivity verification."""

from confluent_kafka.admin import AdminClient
import pytest

from tests.helpers.container_images import KAFKA_IMAGE


@pytest.mark.integration
@pytest.mark.kafka
def test_kafka_broker_reports_cluster_metadata(
    kafka_bootstrap_servers: str,
) -> None:
    """AdminClient can reach the disposable broker and discover a broker node."""
    admin = AdminClient(
        {
            "bootstrap.servers": kafka_bootstrap_servers,
            "client.id": "order-app-test-step-2-connectivity",
            "socket.timeout.ms": 5_000,
        }
    )

    metadata = admin.list_topics(timeout=10)

    assert metadata.brokers, (
        f"Kafka image {KAFKA_IMAGE!r} returned no broker metadata via "
        f"{kafka_bootstrap_servers!r}."
    )
