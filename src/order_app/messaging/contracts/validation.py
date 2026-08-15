"""JSON Schema validation with field-oriented diagnostics."""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from importlib.resources import files
import json
import re
from typing import Any, Mapping

from jsonschema import Draft202012Validator, FormatChecker

from order_app.messaging.contracts.models import OrderCreatedEvent, event_to_wire_dict

_MISSING_PROPERTY = re.compile(r"^'(?P<field>.+)' is a required property$")


@dataclass(frozen=True)
class ValidationIssue:
    """One schema violation tied to a JSON-style field path."""

    path: str
    message: str
    rule: str


class ContractValidationError(ValueError):
    """Raised when a wire event violates its versioned JSON Schema."""

    def __init__(self, issues: tuple[ValidationIssue, ...]) -> None:
        self.issues = issues
        summary = "; ".join(f"{issue.path}: {issue.message}" for issue in issues)
        super().__init__(f"Event contract validation failed: {summary}")


@lru_cache(maxsize=1)
def load_order_created_schema() -> dict[str, Any]:
    """Load and verify the packaged Draft 2020-12 schema once per process."""
    schema_path = files("order_app.messaging.contracts.schemas").joinpath(
        "order-created-v1.json"
    )
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    return schema


@lru_cache(maxsize=1)
def _order_created_validator() -> Draft202012Validator:
    return Draft202012Validator(
        load_order_created_schema(),
        format_checker=FormatChecker(),
    )


def validate_order_created_contract(
    payload: Mapping[str, Any] | OrderCreatedEvent,
) -> None:
    """Raise one error containing every observed wire-contract violation."""
    wire_payload = (
        event_to_wire_dict(payload)
        if isinstance(payload, OrderCreatedEvent)
        else dict(payload)
    )
    issues = tuple(
        sorted(
            (_to_issue(error) for error in _order_created_validator().iter_errors(wire_payload)),
            key=lambda issue: (issue.path, issue.message),
        )
    )
    if issues:
        raise ContractValidationError(issues)


def parse_order_created_event(payload: Mapping[str, Any]) -> OrderCreatedEvent:
    """Validate the wire contract, then return its strict typed representation."""
    validate_order_created_contract(payload)
    return OrderCreatedEvent.model_validate_json(json.dumps(dict(payload)))


def _to_issue(error: Any) -> ValidationIssue:
    path_parts = list(error.absolute_path)
    message = error.message

    if error.validator == "required":
        missing_property = _MISSING_PROPERTY.match(message)
        if missing_property:
            path_parts.append(missing_property.group("field"))
            message = "is required"

    return ValidationIssue(
        path=_json_path(path_parts),
        message=message,
        rule=str(error.validator),
    )


def _json_path(parts: list[object]) -> str:
    path = "$"
    for part in parts:
        path += f"[{part}]" if isinstance(part, int) else f".{part}"
    return path

