"""Smoke tests for the initial project structure."""

import pytest

import order_app
import order_app.messaging


@pytest.mark.unit
def test_application_and_testkit_packages_are_importable() -> None:
    """The order application and its testkit are separate packages."""
    assert order_app.__doc__
    assert order_app.messaging.__doc__
