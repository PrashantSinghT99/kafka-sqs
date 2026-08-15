"""FastAPI producer service used by the component-testing lessons."""

from __future__ import annotations

from collections.abc import Callable
import os
from typing import Annotated, Protocol
from uuid import UUID, uuid4

from fastapi import FastAPI, Header, HTTPException, Response, status
from pydantic import BaseModel, ConfigDict, Field

from order_app.messaging.contracts import OrderCreatedEvent, make_order_created_event
from order_app.messaging.kafka import (
    KafkaEventProducer,
    KafkaPublishError,
    ProducerSettings,
    PublishedRecord,
)


class CreateOrderRequest(BaseModel):
    """Validated business input accepted by the sample producer API."""

    model_config = ConfigDict(extra="forbid", strict=True)

    customer_id: str = Field(min_length=1)
    amount: float = Field(gt=0)
    currency: str = Field(pattern=r"^[A-Z]{3}$")


class CreateOrderResponse(BaseModel):
    """Identifiers returned immediately after Kafka acknowledges the event."""

    model_config = ConfigDict(extra="forbid", strict=True)

    order_id: str
    correlation_id: str
    event_id: UUID


class OrderEventPublisher(Protocol):
    """Small seam that keeps HTTP mapping tests independent of Kafka."""

    def publish_order_created(
        self,
        topic: str,
        event: OrderCreatedEvent,
    ) -> PublishedRecord: ...


OrderIdFactory = Callable[[], str]
IdentifierFactory = Callable[[], str]


def create_order_app(
    publisher: OrderEventPublisher,
    topic: str,
    *,
    order_id_factory: OrderIdFactory | None = None,
    correlation_id_factory: IdentifierFactory | None = None,
    causation_id_factory: IdentifierFactory | None = None,
) -> FastAPI:
    """Create an API whose Kafka boundary can be replaced in unit tests."""
    if not topic.strip():
        raise ValueError("Order event topic must not be blank.")

    new_order_id = order_id_factory or (lambda: f"ORD-{uuid4()}")
    new_correlation_id = correlation_id_factory or (
        lambda: f"checkout-{uuid4()}"
    )
    new_causation_id = causation_id_factory or (lambda: f"request-{uuid4()}")
    app = FastAPI(title="Order Producer API", version="1.0.0")

    @app.post(
        "/orders",
        response_model=CreateOrderResponse,
        status_code=status.HTTP_202_ACCEPTED,
    )
    def create_order(
        order: CreateOrderRequest,
        response: Response,
        correlation_header: Annotated[
            str | None,
            Header(alias="X-Correlation-ID", min_length=1),
        ] = None,
    ) -> CreateOrderResponse:
        correlation_id = correlation_header or new_correlation_id()
        event = make_order_created_event(
            order_id=new_order_id(),
            customer_id=order.customer_id,
            amount=order.amount,
            currency=order.currency,
            correlation_id=correlation_id,
            causation_id=new_causation_id(),
        )
        try:
            publisher.publish_order_created(topic, event)
        except KafkaPublishError as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Order event could not be published.",
            ) from exc

        response.headers["X-Correlation-ID"] = correlation_id
        return CreateOrderResponse(
            order_id=event.data.order_id,
            correlation_id=correlation_id,
            event_id=event.event_id,
        )

    return app


def create_configured_order_app() -> FastAPI:
    """Build the runnable service from explicit environment configuration."""
    bootstrap_servers = _required_environment("KAFKA_BOOTSTRAP_SERVERS")
    topic = _required_environment("ORDER_EVENTS_TOPIC")
    publisher = KafkaEventProducer(ProducerSettings(bootstrap_servers))
    return create_order_app(publisher, topic)


def _required_environment(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Required environment variable {name} is missing.")
    return value
