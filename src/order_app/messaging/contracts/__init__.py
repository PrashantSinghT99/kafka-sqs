"""Versioned event contracts and wire-format validation."""

from order_app.messaging.contracts.factory import make_order_created_event
from order_app.messaging.contracts.models import EventEnvelope, OrderCreatedData, OrderCreatedEvent
from order_app.messaging.contracts.validation import (
    ContractValidationError,
    ValidationIssue,
    load_order_created_schema,
    parse_order_created_event,
    validate_order_created_contract,
)

__all__ = [
    "ContractValidationError",
    "EventEnvelope",
    "OrderCreatedData",
    "OrderCreatedEvent",
    "ValidationIssue",
    "load_order_created_schema",
    "make_order_created_event",
    "parse_order_created_event",
    "validate_order_created_contract",
]

