"""Sample Kafka business consumer and its PostgreSQL side effect."""

from order_app.order_consumer.consumer import (
    ConsumerSettings,
    KafkaOrderConsumer,
    OrderConsumerError,
    OrderConsumerTimeout,
    ProcessedKafkaRecord,
)
from order_app.order_consumer.downstream import (
    DownstreamNotificationError,
    OrderNotificationClient,
)
from order_app.order_consumer.store import (
    EventStoreResult,
    PostgresOrderStore,
    StoredOrder,
)
from order_app.order_consumer.reliability import (
    DeadLetterFailure,
    KafkaDeadLetterPublisher,
    RetryPolicy,
)
from order_app.order_consumer.sqs_consumer import SqsOrderConsumer

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
    "SqsOrderConsumer",
    "StoredOrder",
]
