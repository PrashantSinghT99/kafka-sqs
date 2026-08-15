"""Unit tests for infrastructure readiness diagnostics."""

from __future__ import annotations

import pytest

import tests.helpers.docker as docker_readiness


class _ReadyDockerClient:
    def __init__(self) -> None:
        self.closed = False

    def ping(self) -> bool:
        return True

    def close(self) -> None:
        self.closed = True


@pytest.mark.unit
def test_require_docker_closes_a_ready_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _ReadyDockerClient()
    monkeypatch.setattr(docker_readiness.docker, "from_env", lambda: client)

    docker_readiness.require_docker()

    assert client.closed is True


@pytest.mark.unit
def test_require_docker_explains_how_to_recover(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_to_connect() -> None:
        raise OSError("named pipe is unavailable")

    monkeypatch.setattr(docker_readiness.docker, "from_env", fail_to_connect)

    with pytest.raises(
        docker_readiness.DockerUnavailableError,
        match="Start Docker Desktop",
    ):
        docker_readiness.require_docker()
