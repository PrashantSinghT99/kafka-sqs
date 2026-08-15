"""Visual dashboard and two real producer APIs for local learning mode."""

from __future__ import annotations

from functools import lru_cache
from importlib import resources
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse

from order_app.messaging import KafkaEventPublisher, KafkaTopicAdmin, KafkaPublisherConfig
from order_app.messaging import SqsEventPublisher
from order_app.local_lab.consumer_control_store import ConsumerControlStore
from order_app.local_lab.resource_setup import build_sqs_client, get_queue_url
from order_app.local_lab.config import LocalLabConfig
from order_app.order_api import create_order_app
from order_app.order_processing import PostgresOrderStore, StoredOrder


def create_local_lab_app() -> FastAPI:
    settings = LocalLabConfig.from_environment()
    sqs_client = build_sqs_client(settings)
    sqs_queue_url = get_queue_url(sqs_client, settings.sqs_queue_name)
    kafka_store = PostgresOrderStore(
        settings.postgres_dsn,
        schema=settings.kafka_schema,
    )
    sqs_store = PostgresOrderStore(
        settings.postgres_dsn,
        schema=settings.sqs_schema,
    )
    kafka_store.initialize()
    sqs_store.initialize()
    kafka_admin = KafkaTopicAdmin(settings.kafka_bootstrap_servers)
    controls = ConsumerControlStore(settings.postgres_dsn)
    controls.initialize()

    app = FastAPI(
        title="Message Journey Local Lab",
        version="1.0.0",
        description="Visual Kafka and SQS producer-to-consumer learning mode.",
    )

    @app.get("/", response_class=HTMLResponse, include_in_schema=False)
    def dashboard() -> str:
        return _dashboard_html()

    @app.get("/api/health")
    def health() -> dict[str, Any]:
        services: dict[str, dict[str, str]] = {}
        try:
            metadata = kafka_admin.describe_topic(settings.kafka_topic)
            services["kafka"] = {
                "status": "up",
                "detail": f"{metadata.name} · {metadata.partition_count} partitions",
            }
        except Exception as exc:
            services["kafka"] = {
                "status": "down",
                "detail": _safe_error(exc),
            }

        try:
            queue_attributes = sqs_client.get_queue_attributes(
                QueueUrl=sqs_queue_url,
                AttributeNames=["ApproximateNumberOfMessages"],
            )["Attributes"]
            services["sqs"] = {
                "status": "up",
                "detail": (
                    f"{settings.sqs_queue_name} · "
                    f"{queue_attributes.get('ApproximateNumberOfMessages', '0')} ready"
                ),
            }
        except Exception as exc:
            services["sqs"] = {
                "status": "down",
                "detail": _safe_error(exc),
            }

        try:
            total = kafka_store.order_count() + sqs_store.order_count()
            services["postgres"] = {
                "status": "up",
                "detail": f"order-app · {total} processed orders",
            }
        except Exception as exc:
            services["postgres"] = {
                "status": "down",
                "detail": _safe_error(exc),
            }

        return {
            "status": (
                "ready"
                if all(item["status"] == "up" for item in services.values())
                else "degraded"
            ),
            "services": services,
            "consumers": controls.states(),
            "resources": {
                "kafka_topic": settings.kafka_topic,
                "kafka_dlq_topic": settings.kafka_dlq_topic,
                "kafka_consumer_group": settings.kafka_consumer_group,
                "sqs_queue": settings.sqs_queue_name,
                "sqs_dlq": settings.sqs_dlq_name,
                "kafka_schema": settings.kafka_schema,
                "sqs_schema": settings.sqs_schema,
                "console_url": settings.console_url,
                "adminer_url": settings.adminer_url,
            },
        }

    @app.post("/api/consumers/{broker}/{action}")
    def change_consumer_state(broker: str, action: str) -> dict[str, object]:
        if broker not in {"kafka", "sqs"} or action not in {"pause", "resume"}:
            raise HTTPException(status_code=404, detail="Unknown consumer control.")
        controls.set_paused(broker, paused=action == "pause")
        return {"broker": broker, "paused": action == "pause"}

    @app.get("/api/state")
    def state() -> dict[str, list[dict[str, object]]]:
        return {
            "kafka": [_order_json(order) for order in kafka_store.list_orders()],
            "sqs": [_order_json(order) for order in sqs_store.list_orders()],
        }

    app.mount(
        "/kafka",
        create_order_app(
            KafkaEventPublisher(KafkaPublisherConfig(settings.kafka_bootstrap_servers)),
            settings.kafka_topic,
        ),
        name="kafka-producer",
    )
    app.mount(
        "/sqs",
        create_order_app(SqsEventPublisher(sqs_client), sqs_queue_url),
        name="sqs-producer",
    )
    return app


def _order_json(order: StoredOrder) -> dict[str, object]:
    return {
        "order_id": order.order_id,
        "customer_id": order.customer_id,
        "amount": float(order.amount),
        "currency": order.currency.strip(),
        "event_id": str(order.source_event_id),
        "correlation_id": order.correlation_id,
    }


def _safe_error(exc: Exception) -> str:
    return f"{type(exc).__name__}: {str(exc)[:120]}"


@lru_cache(maxsize=1)
def _dashboard_html() -> str:
    return (
        resources.files("order_app.local_lab")
        .joinpath("static/index.html")
        .read_text(encoding="utf-8")
    )
