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

Test markers separate fast tests from future infrastructure suites:

```powershell
python -m pytest -m contract
python -m pytest -m "integration and kafka"
python -m pytest -m "integration and sqs"
```

## Current status

Steps 1–4 are complete. The framework now includes a strict typed `order.created` version 1 model, packaged Draft 2020-12 JSON Schema, deterministic event factory, and field-level contract diagnostics. Step 5, the Kafka producer client, is the next implementation gate.
