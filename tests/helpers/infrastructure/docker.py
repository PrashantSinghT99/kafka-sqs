"""Docker readiness checks with test-friendly diagnostics."""

from __future__ import annotations

import docker
from docker.errors import DockerException


class DockerUnavailableError(RuntimeError):
    """Raised when an infrastructure test cannot reach the Docker daemon."""


def require_docker() -> None:
    """Verify that Docker is reachable before starting test infrastructure."""
    client = None
    try:
        client = docker.from_env()
        if not client.ping():
            raise DockerUnavailableError(
                "Docker responded, but its readiness ping was unsuccessful."
            )
    except DockerUnavailableError:
        raise
    except (DockerException, OSError) as exc:
        raise DockerUnavailableError(
            "Docker is unavailable. Start Docker Desktop and wait until the engine "
            f"is running before executing integration tests. Original error: {exc}"
        ) from exc
    finally:
        if client is not None:
            client.close()

