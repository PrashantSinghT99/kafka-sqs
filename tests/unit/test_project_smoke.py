"""Smoke tests for the initial project structure."""

import pytest

import mqtest
import sample_app


@pytest.mark.unit
def test_learning_packages_are_importable() -> None:
    """The harness and sample system are separate importable packages."""
    assert mqtest.__version__ == "0.1.0"
    assert sample_app.__doc__

