"""Reusable bounded polling for asynchronous side-effect assertions."""

from __future__ import annotations

from collections.abc import Callable
from threading import Event
from time import monotonic
from typing import Generic, TypeVar


ObservedT = TypeVar("ObservedT")


class EventuallyTimeout(AssertionError, Generic[ObservedT]):
    """Assertion failure containing the final bounded-poll evidence."""

    def __init__(
        self,
        *,
        description: str,
        attempts: int,
        elapsed_seconds: float,
        last_observed: ObservedT,
    ) -> None:
        self.description = description
        self.attempts = attempts
        self.elapsed_seconds = elapsed_seconds
        self.last_observed = last_observed
        super().__init__(
            f"Eventually assertion failed for {description!r} after {attempts} "
            f"attempt(s) and {elapsed_seconds:.3f} seconds; "
            f"last_observed={last_observed!r}."
        )


def eventually(
    observe: Callable[[], ObservedT],
    predicate: Callable[[ObservedT], bool],
    *,
    timeout_seconds: float = 10.0,
    interval_seconds: float = 0.1,
    description: str = "expected condition",
) -> ObservedT:
    """Return as soon as an observation matches, or fail at the deadline."""
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be greater than zero.")
    if interval_seconds <= 0:
        raise ValueError("interval_seconds must be greater than zero.")

    started = monotonic()
    deadline = started + timeout_seconds
    attempts = 0
    last_observed: ObservedT
    waiter = Event()

    while True:
        attempts += 1
        last_observed = observe()
        if predicate(last_observed):
            return last_observed

        remaining = deadline - monotonic()
        if remaining <= 0:
            break
        waiter.wait(min(interval_seconds, remaining))

    raise EventuallyTimeout(
        description=description,
        attempts=attempts,
        elapsed_seconds=monotonic() - started,
        last_observed=last_observed,
    )
