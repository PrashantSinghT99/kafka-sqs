"""Docker readiness checks and pinned integration-test images."""

from __future__ import annotations

import docker
from docker.errors import DockerException


KAFKA_IMAGE = "confluentinc/cp-kafka:7.6.0"
LOCALSTACK_IMAGE = "localstack/localstack:3.5.0"
POSTGRES_IMAGE = "postgres:16.4-alpine"


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
