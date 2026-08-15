"""SQS producer and consumer component boundaries."""

from fastapi.testclient import TestClient
import pytest

from order_app.messaging.contracts import make_order_created_event
from order_app.messaging.sqs import SqsEventClient
from order_app.order_api import create_order_app
from order_app.order_consumer import PostgresOrderStore, SqsOrderConsumer
from tests.helpers.sqs_event_probe import SqsQueueProbe
from tests.helpers.sqs_queues import SqsQueueSet


@pytest.mark.integration
@pytest.mark.sqs
def test_api_publishes_contract_event_to_owned_sqs_queue(
    sqs_client,
    sqs_queues: SqsQueueSet,
) -> None:
    correlation_id = "sqs-api-component-1"
    event_client = SqsEventClient(sqs_client)
    event_probe = SqsQueueProbe(sqs_client)
    app = create_order_app(
        event_client,
        sqs_queues.standard_url,
        order_id_factory=lambda: "ORD-SQS-API-1",
        causation_id_factory=lambda: "request-sqs-api-1",
    )

    with TestClient(app) as client:
        response = client.post(
            "/orders",
            headers={"X-Correlation-ID": correlation_id},
            json={"customer_id": "CUS-SQS-1", "amount": 55.5, "currency": "INR"},
        )
    observed = event_probe.wait_for_event(
        sqs_queues.standard_url,
        correlation_id=correlation_id,
    )

    assert response.status_code == 202
    assert observed.event.data.order_id == "ORD-SQS-API-1"
    assert observed.event.data.customer_id == "CUS-SQS-1"
    attributes = observed.message_attributes
    assert attributes["event-type"]["StringValue"] == "order.created"
    assert attributes["correlation-id"]["StringValue"] == correlation_id


@pytest.mark.integration
@pytest.mark.sqs
def test_sdk_sqs_event_creates_database_effect_then_is_deleted(
    sqs_client,
    sqs_queues: SqsQueueSet,
    order_store: PostgresOrderStore,
) -> None:
    event = make_order_created_event(order_id="ORD-SQS-CONSUMER-1")
    event_client = SqsEventClient(sqs_client)
    event_client.publish_order_created(sqs_queues.standard_url, event)

    processed_id = SqsOrderConsumer(
        sqs_client,
        sqs_queues.standard_url,
        order_store,
    ).process_one()

    assert processed_id == str(event.event_id)
    assert order_store.fetch_order(event.data.order_id) is not None
    assert order_store.has_processed(event.event_id) is True
    remaining = sqs_client.receive_message(
        QueueUrl=sqs_queues.standard_url,
        WaitTimeSeconds=0,
    ).get("Messages", [])
    assert remaining == []
