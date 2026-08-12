"""Transactional PostgreSQL persistence for the sample order consumer."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
import re
from uuid import UUID

import psycopg
from psycopg import sql
from psycopg.rows import class_row

from mqtest.contracts import OrderCreatedEvent


_SCHEMA_NAME = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")


@dataclass(frozen=True)
class StoredOrder:
    """Business state written from one consumed `order.created` event."""

    order_id: str
    customer_id: str
    amount: Decimal
    currency: str
    source_event_id: UUID
    correlation_id: str


class PostgresOrderStore:
    """Own schema setup, atomic event persistence, and observable queries."""

    def __init__(self, dsn: str, *, schema: str = "public") -> None:
        if not _SCHEMA_NAME.fullmatch(schema):
            raise ValueError(f"Unsafe PostgreSQL schema name: {schema!r}.")
        self.dsn = dsn
        self.schema = schema

    def initialize(self) -> None:
        """Create the isolated schema and tables idempotently."""
        schema = sql.Identifier(self.schema)
        with psycopg.connect(self.dsn) as connection:
            connection.execute(
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(schema)
            )
            connection.execute(
                sql.SQL(
                    """
                    CREATE TABLE IF NOT EXISTS {}.orders (
                        order_id TEXT PRIMARY KEY,
                        customer_id TEXT NOT NULL,
                        amount NUMERIC(18, 2) NOT NULL CHECK (amount > 0),
                        currency CHAR(3) NOT NULL,
                        source_event_id UUID NOT NULL UNIQUE,
                        correlation_id TEXT NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                ).format(schema)
            )
            connection.execute(
                sql.SQL(
                    """
                    CREATE TABLE IF NOT EXISTS {}.processed_events (
                        event_id UUID PRIMARY KEY,
                        event_type TEXT NOT NULL,
                        processed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                ).format(schema)
            )

    def store(self, event: OrderCreatedEvent) -> None:
        """Write business data and processing identity in one transaction."""
        schema = sql.Identifier(self.schema)
        with psycopg.connect(self.dsn) as connection:
            connection.execute(
                sql.SQL(
                    """
                    INSERT INTO {}.orders (
                        order_id,
                        customer_id,
                        amount,
                        currency,
                        source_event_id,
                        correlation_id
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    """
                ).format(schema),
                (
                    event.data.order_id,
                    event.data.customer_id,
                    Decimal(str(event.data.amount)),
                    event.data.currency,
                    event.event_id,
                    event.correlation_id,
                ),
            )
            connection.execute(
                sql.SQL(
                    """
                    INSERT INTO {}.processed_events (event_id, event_type)
                    VALUES (%s, %s)
                    """
                ).format(schema),
                (event.event_id, event.event_type),
            )

    def fetch_order(self, order_id: str) -> StoredOrder | None:
        """Return the observable business record for an assertion."""
        with psycopg.connect(
            self.dsn,
            row_factory=class_row(StoredOrder),
        ) as connection:
            return connection.execute(
                sql.SQL(
                    """
                    SELECT order_id, customer_id, amount, currency,
                           source_event_id, correlation_id
                    FROM {}.orders
                    WHERE order_id = %s
                    """
                ).format(sql.Identifier(self.schema)),
                (order_id,),
            ).fetchone()

    def has_processed(self, event_id: UUID) -> bool:
        """Report whether the consumer transaction recorded this event ID."""
        with psycopg.connect(self.dsn) as connection:
            row = connection.execute(
                sql.SQL(
                    "SELECT EXISTS (SELECT 1 FROM {}.processed_events "
                    "WHERE event_id = %s)"
                ).format(sql.Identifier(self.schema)),
                (event_id,),
            ).fetchone()
        return bool(row and row[0])

    def drop_processed_events_table(self) -> None:
        """Create a deterministic database failure for the crash-window test."""
        with psycopg.connect(self.dsn) as connection:
            connection.execute(
                sql.SQL("DROP TABLE {}.processed_events").format(
                    sql.Identifier(self.schema)
                )
            )

    def drop_schema(self) -> None:
        """Remove all function-scoped PostgreSQL state."""
        if self.schema == "public":
            raise ValueError("The shared public schema cannot be dropped.")
        with psycopg.connect(self.dsn) as connection:
            connection.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                    sql.Identifier(self.schema)
                )
            )
