"""Unit tests for bounded asynchronous assertions."""

import pytest

from mqtest import EventuallyTimeout, eventually


@pytest.mark.unit
def test_eventually_returns_immediately_when_first_observation_matches() -> None:
    attempts = 0

    def observe() -> str:
        nonlocal attempts
        attempts += 1
        return "ready"

    result = eventually(
        observe,
        lambda value: value == "ready",
        timeout_seconds=1,
        interval_seconds=0.01,
        description="service readiness",
    )

    assert result == "ready"
    assert attempts == 1


@pytest.mark.unit
def test_eventually_retries_until_later_observation_matches() -> None:
    values = iter([None, None, "order"])

    result = eventually(
        lambda: next(values),
        lambda value: value is not None,
        timeout_seconds=1,
        interval_seconds=0.001,
        description="order row",
    )

    assert result == "order"


@pytest.mark.unit
def test_eventually_timeout_reports_attempts_elapsed_and_last_value() -> None:
    with pytest.raises(EventuallyTimeout) as raised:
        eventually(
            lambda: {"state": "pending"},
            lambda value: value["state"] == "complete",
            timeout_seconds=0.01,
            interval_seconds=0.002,
            description="order completion",
        )

    error = raised.value
    assert error.attempts >= 2
    assert error.elapsed_seconds >= 0.01
    assert error.last_observed == {"state": "pending"}
    assert "order completion" in str(error)


@pytest.mark.unit
@pytest.mark.parametrize("field", ["timeout", "interval"])
def test_eventually_rejects_non_positive_bounds(field: str) -> None:
    options = {"timeout_seconds": 1.0, "interval_seconds": 0.1}
    options[f"{field}_seconds"] = 0

    with pytest.raises(ValueError, match="greater than zero"):
        eventually(lambda: None, lambda _: False, **options)
