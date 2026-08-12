# Kafka and SQS Test Automation Learning Framework

This repository is a hands-on project for learning how an SDET architect tests asynchronous producer and consumer systems without depending on a UI.

The project will build a Python/pytest test framework around real Kafka and SQS-compatible infrastructure. Kafka is implemented first so that topic, partition, consumer-group, and offset behavior can be learned before adding SQS visibility and acknowledgement behavior.

## What we will test

- Producer tests: trigger an API and independently verify the event published to Kafka or SQS.
- Consumer tests: publish a controlled event through an SDK and verify database, HTTP, emitted-event, retry, and DLQ effects.
- Contract tests: validate the event envelope, payload schema, and compatible evolution.
- Reliability tests: validate duplicate delivery, idempotency, ordering, retry, recovery, and poison-message handling.

## Agreed design

- [Architecture](docs/architecture.md)
- [Step-by-step implementation plan](docs/implementation-plan.md)
- [Decisions and implementation progress](docs/decisions.md)

## Local setup

Prerequisites:

- Python 3.12
- Docker Desktop is required for integration tests, but not for unit tests.

PowerShell setup:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
```

Run the current unit suite:

```powershell
python -m pytest -m unit
```

Run the Kafka integration suite after Docker Desktop reports that its engine is
running:

```powershell
python -m pytest -m "integration and kafka"
```

The integration suite starts and removes its own pinned Kafka container. You do
not need to install Kafka on the host.

Run the sample producer API against a configured Kafka topic:

```powershell
$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
$env:ORDER_EVENTS_TOPIC = "orders.created"
uvicorn --factory sample_app.order_api:create_configured_order_app
```

The API accepts `POST /orders`, propagates an optional `X-Correlation-ID`, and
returns `202 Accepted` only after Kafka acknowledges the event.

Test markers separate fast tests from future infrastructure suites:

```powershell
python -m pytest -m contract
python -m pytest -m "integration and kafka"
python -m pytest -m "integration and sqs"
```

## Kafka probe example

Start the probe before triggering the system under test. Its unique consumer
group observes the topic independently and never commits application offsets:

```python
settings = ProbeSettings(kafka_bootstrap_servers)
with KafkaEventProbe(settings, topic_name) as probe:
    trigger_the_producer(correlation_id="checkout-123")
    record = probe.wait_for_event(
        match_order_created_event(correlation_id="checkout-123"),
        timeout_seconds=10,
    )

assert record.event.data.order_id == "ORD-123"
```

## Current status

Steps 1–15 are complete. A pinned LocalStack SQS service now provides function-owned standard, FIFO, and DLQ queues with explicit visibility, long-poll, redrive, group, and deduplication settings. Queue names preserve unique role identity even when long pytest IDs are truncated. Step 16 adds SQS producer and consumer component tests.
