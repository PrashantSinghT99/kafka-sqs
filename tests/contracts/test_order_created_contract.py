"""Executable contract examples for `order.created` version 1."""

import pytest

from order_app.messaging.contracts import (
    ContractValidationError,
    ValidationIssue,
    make_order_created_event,
    parse_order_created_event,
    validate_order_created_contract,
)
from order_app.messaging.contracts.models import event_to_wire_dict


@pytest.mark.contract
def test_valid_order_created_event_serializes_and_validates() -> None:
    event = make_order_created_event(correlation_id="checkout-123")
    payload = event_to_wire_dict(event)

    validate_order_created_contract(payload)
    parsed = parse_order_created_event(payload)

    assert parsed == event
    assert payload["event_type"] == "order.created"
    assert payload["event_version"] == 1


@pytest.mark.contract
def test_missing_event_id_has_field_level_diagnostic() -> None:
    payload = event_to_wire_dict(make_order_created_event())
    payload.pop("event_id")

    with pytest.raises(ContractValidationError) as captured:
        validate_order_created_contract(payload)

    assert captured.value.issues == (
        ValidationIssue("$.event_id", "is required", "required"),
    )


@pytest.mark.contract
def test_wrong_amount_type_identifies_nested_field() -> None:
    payload = event_to_wire_dict(make_order_created_event())
    payload["data"]["amount"] = "500.00"

    with pytest.raises(ContractValidationError) as captured:
        validate_order_created_contract(payload)

    assert captured.value.issues[0].path == "$.data.amount"
    assert captured.value.issues[0].rule == "type"


@pytest.mark.contract
def test_unsupported_event_version_is_rejected() -> None:
    payload = event_to_wire_dict(make_order_created_event())
    payload["event_version"] = 2

    with pytest.raises(ContractValidationError) as captured:
        validate_order_created_contract(payload)

    assert captured.value.issues[0].path == "$.event_version"
    assert captured.value.issues[0].rule == "const"


@pytest.mark.contract
def test_event_id_correlation_id_and_causation_id_have_distinct_roles() -> None:
    first = make_order_created_event(
        correlation_id="checkout-123",
        causation_id="request-001",
    )
    second = make_order_created_event(
        correlation_id="checkout-123",
        causation_id="request-002",
    )

    assert first.event_id != second.event_id
    assert first.correlation_id == second.correlation_id
    assert first.causation_id != second.causation_id
