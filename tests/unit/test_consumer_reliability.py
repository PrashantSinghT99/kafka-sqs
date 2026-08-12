"""Unit tests for explicit retry classification and bounds."""

import pytest

from sample_app.order_consumer import (
    DownstreamNotificationError,
    RetryPolicy,
)


@pytest.mark.unit
def test_retry_policy_classifies_only_declared_errors() -> None:
    policy = RetryPolicy(
        max_attempts=3,
        backoff_seconds=0.25,
        retryable_errors=(DownstreamNotificationError,),
    )

    assert policy.is_retryable(DownstreamNotificationError("temporary")) is True
    assert policy.is_retryable(ValueError("invalid contract")) is False
    assert policy.max_attempts == 3
    assert policy.backoff_seconds == 0.25


@pytest.mark.unit
@pytest.mark.parametrize(
    ("max_attempts", "backoff_seconds"),
    [(0, 0), (1, -0.1)],
)
def test_retry_policy_rejects_invalid_bounds(
    max_attempts: int,
    backoff_seconds: float,
) -> None:
    with pytest.raises(ValueError):
        RetryPolicy(
            max_attempts=max_attempts,
            backoff_seconds=backoff_seconds,
        )
