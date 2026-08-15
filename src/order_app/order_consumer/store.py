"""Transactional PostgreSQL persistence for the sample order consumer."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
import re
from uuid import UUID

import psycopg
from psycopg import sql
from psycopg.rows import class_row

from order_app.messaging.contracts import OrderCreatedEvent


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


@dataclass(frozen=True)
class EventStoreResult:
    """Atomic decision returned for new, pending, and completed deliveries."""

    is_new: bool
    downstream_required: bool


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
                        status TEXT NOT NULL DEFAULT 'pending'
                            CHECK (status IN ('pending', 'completed')),
                        first_seen_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        processed_at TIMESTAMPTZ NULL
                    )
                    """
                ).format(schema)
            )

    def store(self, event: OrderCreatedEvent) -> EventStoreResult:
        """Atomically claim a new event and write its business state once."""
        schema = sql.Identifier(self.schema)
        with psycopg.connect(self.dsn) as connection:
            claimed = connection.execute(
                sql.SQL(
                    """
                    INSERT INTO {}.processed_events (event_id, event_type)
                    VALUES (%s, %s)
                    ON CONFLICT (event_id) DO NOTHING
                    RETURNING event_id
                    """
                ).format(schema),
                (event.event_id, event.event_type),
            ).fetchone()

            if claimed is None:
                existing = connection.execute(
                    sql.SQL(
                        "SELECT status FROM {}.processed_events "
                        "WHERE event_id = %s FOR UPDATE"
                    ).format(schema),
                    (event.event_id,),
                ).fetchone()
                if existing is None:
                    raise RuntimeError(
                        f"Event claim disappeared for {event.event_id}."
                    )
                return EventStoreResult(
                    is_new=False,
                    downstream_required=existing[0] != "completed",
                )

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
            return EventStoreResult(is_new=True, downstream_required=True)

    def mark_completed(self, event_id: UUID) -> None:
        """Mark all required effects complete before Kafka offset commit."""
        with psycopg.connect(self.dsn) as connection:
            result = connection.execute(
                sql.SQL(
                    """
                    UPDATE {}.processed_events
                    SET status = 'completed', processed_at = CURRENT_TIMESTAMP
                    WHERE event_id = %s
                    """
                ).format(sql.Identifier(self.schema)),
                (event_id,),
            )
            if result.rowcount != 1:
                raise RuntimeError(
                    f"Cannot complete unknown event {event_id}."
                )

    def discard(self, event_id: UUID) -> None:
        """Atomically remove partial state before terminal dead-lettering."""
        schema = sql.Identifier(self.schema)
        with psycopg.connect(self.dsn) as connection:
            connection.execute(
                sql.SQL("DELETE FROM {}.orders WHERE source_event_id = %s").format(
                    schema
                ),
                (event_id,),
            )
            connection.execute(
                sql.SQL("DELETE FROM {}.processed_events WHERE event_id = %s").format(
                    schema
                ),
                (event_id,),
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

    def list_orders(self, *, limit: int = 25) -> list[StoredOrder]:
        """Return recent business state for the local visual dashboard."""
        if limit <= 0:
            raise ValueError("limit must be greater than zero.")
        with psycopg.connect(
            self.dsn,
            row_factory=class_row(StoredOrder),
        ) as connection:
            rows = connection.execute(
                sql.SQL(
                    """
                    SELECT order_id, customer_id, amount, currency,
                           source_event_id, correlation_id
                    FROM {}.orders
                    ORDER BY created_at DESC
                    LIMIT %s
                    """
                ).format(sql.Identifier(self.schema)),
                (limit,),
            ).fetchall()
        return list(rows)

    def has_processed(self, event_id: UUID) -> bool:
        """Report whether the consumer transaction recorded this event ID."""
        with psycopg.connect(self.dsn) as connection:
            row = connection.execute(
                sql.SQL(
                    "SELECT EXISTS (SELECT 1 FROM {}.processed_events "
                    "WHERE event_id = %s AND status = 'completed')"
                ).format(sql.Identifier(self.schema)),
                (event_id,),
            ).fetchone()
        return bool(row and row[0])

    def order_count(self) -> int:
        with psycopg.connect(self.dsn) as connection:
            row = connection.execute(
                sql.SQL("SELECT COUNT(*) FROM {}.orders").format(
                    sql.Identifier(self.schema)
                )
            ).fetchone()
        return int(row[0]) if row else 0

    def processed_event_count(self) -> int:
        with psycopg.connect(self.dsn) as connection:
            row = connection.execute(
                sql.SQL("SELECT COUNT(*) FROM {}.processed_events").format(
                    sql.Identifier(self.schema)
                )
            ).fetchone()
        return int(row[0]) if row else 0

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
