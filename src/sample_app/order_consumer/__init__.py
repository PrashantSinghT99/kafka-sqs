"""Sample Kafka business consumer and its PostgreSQL side effect."""

from sample_app.order_consumer.consumer import (
    ConsumerSettings,
    KafkaOrderConsumer,
    OrderConsumerError,
    OrderConsumerTimeout,
    ProcessedKafkaRecord,
)
from sample_app.order_consumer.downstream import (
    DownstreamNotificationError,
    OrderNotificationClient,
)
from sample_app.order_consumer.store import (
    EventStoreResult,
    PostgresOrderStore,
    StoredOrder,
)
from sample_app.order_consumer.reliability import (
    DeadLetterFailure,
    KafkaDeadLetterPublisher,
    RetryPolicy,
)

__all__ = [
    "ConsumerSettings",
    "DeadLetterFailure",
    "DownstreamNotificationError",
    "EventStoreResult",
    "KafkaOrderConsumer",
    "KafkaDeadLetterPublisher",
    "OrderConsumerError",
    "OrderConsumerTimeout",
    "OrderNotificationClient",
    "PostgresOrderStore",
    "ProcessedKafkaRecord",
    "RetryPolicy",
    "StoredOrder",
]
