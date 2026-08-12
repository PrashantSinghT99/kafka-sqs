"""Compose one-shot entry point for local resource initialization."""

from sample_app.local_lab.infrastructure import initialize_lab
from sample_app.local_lab.settings import LocalLabSettings


def main() -> None:
    initialize_lab(LocalLabSettings.from_environment())


if __name__ == "__main__":
    main()
