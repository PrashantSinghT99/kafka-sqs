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

## Visual local learning mode

Start a persistent end-to-end system with Apache Kafka, Redpanda Console,
LocalStack SQS, PostgreSQL, Adminer, two continuous consumers, and a visual
Message Journey dashboard:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\local-lab.ps1 Start
```

The dashboard opens at `http://127.0.0.1:8000`. It can pause either consumer so
you can watch a message wait in Kafka or SQS, then resume processing and see the
PostgreSQL result. Follow the complete [visual local lab walkthrough](docs/local-lab.md).

Stop while preserving data:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\local-lab.ps1 Stop
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

## Continuous integration

GitHub Actions runs three independent gates:

1. Unit and contract tests without Docker.
2. Kafka/PostgreSQL integration and reliability tests.
3. LocalStack SQS/PostgreSQL integration and reliability tests.

The Docker-backed jobs start only after the fast gate passes. Every job has a
hard timeout and uploads its JUnit XML even on failure. GitHub-hosted Ubuntu
runners provide the Docker daemon required by Testcontainers; self-hosted
runners must provide a compatible Docker Engine and enough space for the pinned
Kafka, PostgreSQL, and LocalStack images.

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

All 18 automated-framework baseline steps plus the Step 19 visual local-lab extension are complete. The framework independently tests Kafka and SQS producers/consumers, contracts, database and HTTP effects, idempotency, retry/DLQ, ordering, recovery, visibility, redrive, FIFO behavior, and deduplication. GitHub Actions provides fast, Kafka, and SQS gates with JUnit evidence and bounded job timeouts. The complete local baseline currently contains 81 passing tests.
