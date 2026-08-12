"""Fast guards for the persistent visual learning mode."""

from pathlib import Path

import pytest
import yaml

from mqtest.kafka import TopicSpec
from sample_app.local_lab.app import _dashboard_html
from sample_app.local_lab.control import ConsumerControls
from sample_app.local_lab.infrastructure import ensure_topic
from sample_app.local_lab.settings import LocalLabSettings
from sample_app.order_consumer import PostgresOrderStore


ROOT = Path(__file__).parents[2]


@pytest.mark.unit
def test_local_settings_have_stable_learning_resources(monkeypatch) -> None:
    names = (
        "KAFKA_BOOTSTRAP_SERVERS",
        "KAFKA_TOPIC",
        "KAFKA_DLQ_TOPIC",
        "KAFKA_CONSUMER_GROUP",
        "SQS_ENDPOINT_URL",
        "SQS_QUEUE_NAME",
        "SQS_DLQ_NAME",
        "POSTGRES_DSN",
    )
    for name in names:
        monkeypatch.delenv(name, raising=False)

    settings = LocalLabSettings.from_environment()

    assert settings.kafka_topic == "orders.created.local"
    assert settings.kafka_consumer_group == "local-order-consumer"
    assert settings.sqs_queue_name == "orders-created-local"
    assert settings.kafka_schema == "kafka_lab"
    assert settings.sqs_schema == "sqs_lab"


@pytest.mark.unit
def test_local_dashboard_exposes_both_message_journeys() -> None:
    html = _dashboard_html()

    assert "Message Journey Lab" in html
    assert 'data-broker="kafka"' in html
    assert 'data-broker="sqs"' in html
    assert 'data-control="kafka"' in html
    assert 'data-control="sqs"' in html
    assert "Open Kafka Console" in html
    assert "Open PostgreSQL UI" in html


@pytest.mark.unit
def test_local_compose_binds_all_host_ports_to_loopback() -> None:
    compose = yaml.safe_load((ROOT / "compose.local.yml").read_text(encoding="utf-8"))
    services = compose["services"]

    assert {
        "kafka",
        "kafka-console",
        "postgres",
        "adminer",
        "localstack",
        "lab-init",
        "kafka-worker",
        "sqs-worker",
        "dashboard",
    } <= services.keys()
    exposed = [
        port
        for service in services.values()
        for port in service.get("ports", [])
    ]
    assert exposed
    assert all(str(port).startswith("127.0.0.1:") for port in exposed)
    assert services["kafka"]["environment"]["KAFKA_AUTO_CREATE_TOPICS_ENABLE"] == "false"


@pytest.mark.unit
def test_topic_initializer_is_idempotent() -> None:
    class FakeAdmin:
        def __init__(self) -> None:
            self.created: list[TopicSpec] = []

        def list_topic_names(self) -> frozenset[str]:
            return frozenset({"already-there"})

        def create_topic(self, spec: TopicSpec) -> None:
            self.created.append(spec)

    admin = FakeAdmin()
    ensure_topic(admin, "already-there")  # type: ignore[arg-type]
    ensure_topic(admin, "orders.created.local")  # type: ignore[arg-type]

    assert [spec.name for spec in admin.created] == ["orders.created.local"]
    assert admin.created[0].partition_count == 3


@pytest.mark.unit
def test_consumer_controls_and_order_listing_reject_invalid_input() -> None:
    controls = ConsumerControls("postgresql://unused")
    with pytest.raises(ValueError, match="Unsupported"):
        controls.is_paused("rabbitmq")

    store = PostgresOrderStore("postgresql://unused")
    with pytest.raises(ValueError, match="greater than zero"):
        store.list_orders(limit=0)
