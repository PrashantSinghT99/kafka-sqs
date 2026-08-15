"""Strict typed models for the learning event envelope."""

from __future__ import annotations

from datetime import datetime
from typing import Generic, Literal, TypeVar
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

PayloadT = TypeVar("PayloadT", bound=BaseModel)


class StrictContractModel(BaseModel):
    """Base model that rejects coercion and unknown wire-contract fields."""

    model_config = ConfigDict(extra="forbid", strict=True)


class EventEnvelope(StrictContractModel, Generic[PayloadT]):
    """Metadata shared by every event in one asynchronous system."""

    event_id: UUID
    event_type: str = Field(min_length=1)
    event_version: int = Field(ge=1)
    occurred_at: AwareDatetime
    correlation_id: str = Field(min_length=1)
    causation_id: str = Field(min_length=1)
    producer: str = Field(min_length=1)
    data: PayloadT


class OrderCreatedData(StrictContractModel):
    """Business payload carried by an `order.created` event."""

    order_id: str = Field(min_length=1)
    customer_id: str = Field(min_length=1)
    amount: float = Field(gt=0)
    currency: str = Field(pattern=r"^[A-Z]{3}$")


class OrderCreatedEvent(EventEnvelope[OrderCreatedData]):
    """Version 1 of the `order.created` event."""

    event_type: Literal["order.created"] = "order.created"
    event_version: Literal[1] = 1
    producer: Literal["order-api"] = "order-api"


def event_to_wire_dict(event: EventEnvelope[PayloadT]) -> dict[str, object]:
    """Serialize a typed event into JSON-compatible values."""
    return event.model_dump(mode="json")


def ensure_utc_datetime(value: datetime) -> datetime:
    """Keep factory timestamp validation explicit and independently testable."""
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Event timestamps must include timezone information.")
    return value

