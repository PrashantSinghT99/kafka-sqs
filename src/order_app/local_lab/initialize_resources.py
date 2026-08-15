"""Compose one-shot entry point for local resource initialization."""

from order_app.local_lab.resource_setup import initialize_lab
from order_app.local_lab.config import LocalLabConfig


def main() -> None:
    initialize_lab(LocalLabConfig.from_environment())


if __name__ == "__main__":
    main()
