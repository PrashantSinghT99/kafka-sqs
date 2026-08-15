"""PostgreSQL-backed pause controls shared by dashboard and workers."""

from __future__ import annotations

import psycopg


_BROKERS = frozenset({"kafka", "sqs"})


class ConsumerControlStore:
    def __init__(self, dsn: str) -> None:
        self.dsn = dsn

    def initialize(self) -> None:
        with psycopg.connect(self.dsn) as connection:
            connection.execute("CREATE SCHEMA IF NOT EXISTS local_lab")
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS local_lab.consumer_controls (
                    broker TEXT PRIMARY KEY CHECK (broker IN ('kafka', 'sqs')),
                    paused BOOLEAN NOT NULL DEFAULT FALSE,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            with connection.cursor() as cursor:
                cursor.executemany(
                    """
                    INSERT INTO local_lab.consumer_controls (broker)
                    VALUES (%s)
                    ON CONFLICT (broker) DO NOTHING
                    """,
                    [(broker,) for broker in sorted(_BROKERS)],
                )

    def set_paused(self, broker: str, *, paused: bool) -> None:
        _validate_broker(broker)
        with psycopg.connect(self.dsn) as connection:
            result = connection.execute(
                """
                UPDATE local_lab.consumer_controls
                SET paused = %s, updated_at = CURRENT_TIMESTAMP
                WHERE broker = %s
                """,
                (paused, broker),
            )
            if result.rowcount != 1:
                raise RuntimeError(f"Consumer control is missing for {broker!r}.")

    def is_paused(self, broker: str) -> bool:
        _validate_broker(broker)
        with psycopg.connect(self.dsn) as connection:
            row = connection.execute(
                """
                SELECT paused FROM local_lab.consumer_controls
                WHERE broker = %s
                """,
                (broker,),
            ).fetchone()
        if row is None:
            raise RuntimeError(f"Consumer control is missing for {broker!r}.")
        return bool(row[0])

    def states(self) -> dict[str, bool]:
        with psycopg.connect(self.dsn) as connection:
            rows = connection.execute(
                "SELECT broker, paused FROM local_lab.consumer_controls"
            ).fetchall()
        return {str(broker): bool(paused) for broker, paused in rows}


def _validate_broker(broker: str) -> None:
    if broker not in _BROKERS:
        raise ValueError(f"Unsupported local-lab broker: {broker!r}.")
