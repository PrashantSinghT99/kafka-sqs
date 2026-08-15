# Code Map: What Each Folder and File Does

This map answers three questions for every part of the repository: what it owns, who calls it, and why it exists.

## End-to-end runtime flow

```text
Browser / HTTP client -> order_api/http_api.py
                              |
              +---------------+----------------+
              |                                |
              v                                v
 KafkaEventPublisher                  SqsEventPublisher
              |                                |
              v                                v
         Kafka topic                        SQS queue
              |                                |
              v                                v
 kafka_order_consumer.py             sqs_order_consumer.py
       |              |                       |
       |              v                       |
       |     notification_client.py            |
       |                                      |
       `------------------+-------------------'
                          v
              postgres_order_store.py
                          |
                          v
                      PostgreSQL
```

The Kafka consumer uses both PostgreSQL and the notification client. The current SQS learning path writes PostgreSQL only.

The `local_lab` folder wires these same components into the visual dashboard. It does not contain a second implementation of Kafka or SQS behavior.

## Top-level folders

| Path | Responsibility | Needed by |
|---|---|---|
| `src/order_app` | Installable application and messaging runtime | API, local lab, consumers, and tests |
| `tests/unit` | Fast isolated behavior tests | Developer feedback and CI fast gate |
| `tests/contracts` | Event-schema compatibility tests | Producer/consumer contract safety |
| `tests/integration` | Real Kafka, SQS, PostgreSQL, and HTTP flows | Component and reliability verification |
| `tests/helpers` | Test-only probes, stubs, polling, and disposable resource support | Test cases only; never imported by the application |
| `docs` | Architecture, plan, decisions, and this ownership map | Learners and maintainers |

## Application files

| File | What it does | Plugged into |
|---|---|---|
| `order_api/http_api.py` | Validates an HTTP order request, creates an event, calls a supplied publisher, and returns acknowledgement data | Local dashboard and producer component tests |
| `messaging/contracts/models.py` | Defines the typed event envelope and `order.created` payload | Every publisher, consumer, and contract test |
| `messaging/contracts/factory.py` | Creates valid `order.created` events with IDs and timestamps | HTTP API, dashboard, and tests |
| `messaging/contracts/validation.py` | Loads JSON Schema, validates wire data, and parses it into typed models | Kafka/SQS publishers and consumers |
| `messaging/contracts/schemas/order-created-v1.json` | The language-neutral version-1 wire contract | Loaded by `validation.py` |
| `messaging/event_publishers.py` | Defines the `EventPublisher` base plus Kafka and SQS child publishers, their configuration, receipts, serialization, and broker errors | HTTP API, dashboard, and producer tests |
| `messaging/kafka_topic_admin.py` | Creates, describes, and deletes Kafka topics with bounded waits | Local resource setup and isolated integration fixtures |
| `order_processing/kafka_order_consumer.py` | Reads Kafka records, processes effects, handles retry/DLQ, and commits only after success | Kafka worker and consumer tests |
| `order_processing/sqs_order_consumer.py` | Reads SQS, stores valid orders, and deletes only after success | SQS worker and consumer tests |
| `order_processing/postgres_order_store.py` | Applies transactional, event-ID-based idempotency and exposes stored orders | Both consumers, dashboard, and tests |
| `order_processing/notification_client.py` | Sends the downstream order notification over HTTP | Kafka consumer reliability flows |
| `order_processing/retry_and_dead_letter.py` | Defines retry policy and publishes exhausted Kafka records to a DLQ | Kafka consumer |

## Visual local-lab files

| File | What it does | How it starts |
|---|---|---|
| `local_lab/dashboard_api.py` | Serves the browser UI and Kafka/SQS producer endpoints | Uvicorn command in `compose.local.yml` |
| `local_lab/consumer_worker.py` | Runs either the continuous Kafka worker or SQS worker | Two worker commands in `compose.local.yml` |
| `local_lab/initialize_resources.py` | One-shot executable that invokes resource setup | Initializer command in `compose.local.yml` |
| `local_lab/resource_setup.py` | Idempotently creates topics, queues, database tables, and controls | Initializer and runtime processes |
| `local_lab/consumer_control_store.py` | Stores pause/resume state for the visual exercise in PostgreSQL | Dashboard and both workers |
| `local_lab/config.py` | Reads environment variables into one typed local-lab configuration | All local-lab processes |
| `local_lab/static/index.html` | Browser dashboard markup, styling, and JavaScript | Loaded by `dashboard_api.py` |

## Test-helper files

| File | Why it exists |
|---|---|
| `tests/helpers/kafka.py` | Generates isolated topic names and owns the stateful independent Kafka consumer probe |
| `tests/helpers/sqs.py` | Uses functions to create/delete queue families and wait for a correlated SQS event; data classes only describe returned evidence |
| `tests/helpers/client_stub.py` | Provides bounded asynchronous polling plus the stateful recording HTTP server stub |
| `tests/helpers/docker.py` | Pins Testcontainers images and fails early with a useful Docker-unavailable message |

## Why `__init__.py` files remain

`__init__.py` marks a folder as an importable Python package. In `messaging`, `contracts`, `order_api`, and `order_processing`, it also provides a small public import surface so callers do not depend on every internal filename. These files contain no competing implementation.

## Why the remaining classes are classes

| Kind | Examples | Why a class is justified |
|---|---|---|
| Interchangeable broker behavior | `EventPublisher`, `KafkaEventPublisher`, `SqsEventPublisher` | The HTTP API calls one common operation while each child implements different SDK behavior |
| Resource lifecycle | `KafkaEventProbe`, `RecordingHttpStub` | Each owns a live client/server, mutable observation state, and deterministic cleanup |
| Stateful adapter | `PostgresOrderStore`, `KafkaTopicAdmin`, order consumers | Each retains configuration and an SDK/database client across multiple operations |
| Data/configuration value | `KafkaPublisherConfig`, `SqsQueueSet`, event models | Dataclasses and Pydantic models validate and name structured data; they do not hide procedural behavior |
| Domain-specific failure | `KafkaPublishError`, `EventuallyTimeout` | Exception classes let callers catch a precise failure and preserve diagnostics |

The former `SqsQueueProbe` and `SqsTestResources` wrappers did not own meaningful lifecycle beyond a supplied boto3 client, so they were replaced with `wait_for_sqs_event`, `create_sqs_queue_set`, and `delete_sqs_queue_set` functions.

## Wiring rule

- Runtime code may import only from `src/order_app`.
- Test cases may import runtime code and `tests/helpers`.
- Runtime code must never import `tests/helpers`.
- A new file should have one named responsibility and at least one explicit caller, resource loader, or executable entry point.
- A new class must justify itself through state, lifecycle, validated data, polymorphism, or a distinct exception type; otherwise prefer a function.
