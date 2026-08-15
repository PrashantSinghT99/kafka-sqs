"""Smoke tests for the initial project structure."""

import pytest

import order_app
import order_app.messaging


@pytest.mark.unit
def test_application_runtime_packages_are_importable() -> None:
    """The application and its runtime messaging package are importable."""
    assert order_app.__doc__
    assert order_app.messaging.__doc__
