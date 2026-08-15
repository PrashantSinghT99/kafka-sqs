# Decisions and Implementation Progress

This is the living implementation record for the project. Update it whenever a step begins, a material decision is made, verification is completed, or the next implementation gate changes.

## Current position

| Item | Current value |
|---|---|
| Active phase | Phase 8 — Structure and naming cleanup |
| Current point | Step 21C — Consolidate helpers and broker publishers |
| Step status | Completed and verified |
| Last completed step | Step 21C — Four-file helpers and shared publisher hierarchy |
| Next gate | Walk through `docs/code-map.md`, then map each runtime boundary to its automated tests |

## Repository state

| Item | Current value |
|---|---|
| Primary branch | `main` |
| Remote | `origin` → `https://github.com/PrashantSinghT99/kafka-sqs.git` |
| Last completed-step reference | Step 21A — commit `85c5999` |
| Remote tracking | `main` → `origin/main` |
| Commit policy | One verified implementation-step commit per completed step |

## Milestone history

| Date | Milestone | Status | Verification |
|---|---|---|---|
| 2026-08-12 | Architecture baseline | Completed | Architecture and 18-step plan reviewed for structure and links |
| 2026-08-12 | Step 1 — Python project bootstrap | Completed | Clean `.venv` editable install; 1 unit test passed; 1 test collected |
| 2026-08-12 | Step 2 — Disposable Kafka | Completed | Full suite: 4 passed; Kafka AdminClient connected; container removed after test |
| 2026-08-12 | Step 3 — Isolated topics | Completed | Full suite: 10 passed; metadata evidence verified; topic and broker cleanup passed |
| 2026-08-12 | Git baseline for Steps 1–3 | Completed | Commit `2fa78a2` pushed to `origin/main` |
| 2026-08-12 | Step 4 — Event contract | Completed | 17 tests passed; schema format checks and wheel packaging verified |
| 2026-08-12 | Step 5 — Kafka producer | Completed | 23 tests passed; real broker acknowledgement and delivery evidence verified |
| 2026-08-12 | Step 6 — Kafka test probe | Completed | 30 tests passed; isolated observation, predicate matching, deadline diagnostics, and cleanup verified |
| 2026-08-12 | Step 7 — Sample producer API | Completed | 37 tests passed; validation, event mapping, correlation propagation, and publish-failure mapping verified |
| 2026-08-12 | Step 8 — Producer component test | Completed | 39 tests passed; positive HTTP-to-Kafka record and negative no-publication paths verified |
| 2026-08-12 | Step 9 — Transactional order consumer | Completed | 44 tests passed; Kafka-to-PostgreSQL state, rollback, redelivery, and post-DB offset commit verified |
| 2026-08-12 | Step 10 — Eventual consumer assertions | Completed | 50 tests passed; immediate/retry/timeout polling and asynchronous Kafka-to-PostgreSQL component flow verified |
| 2026-08-12 | Step 11 — Downstream HTTP verification | Completed | 53 tests passed; request contract and scripted temporary-failure recovery verified through a real HTTP stub |
| 2026-08-12 | Step 12 — Consumer idempotency | Completed | 57 tests passed; duplicate suppression, pending-effect recovery, and correlation-ID reuse verified |
| 2026-08-12 | Step 13 — Retry and DLQ | Completed | 62 tests passed; classified retries, poison cleanup, DLQ evidence, and later-record continuation verified |
| 2026-08-12 | Step 14 — Kafka ordering/recovery/transactions | Completed | 66 tests passed; partition ordering, group restart, and committed-transaction visibility verified |
| 2026-08-12 | Step 15 — LocalStack SQS resources | Completed | 70 tests passed; standard/FIFO/DLQ attributes, redrive wiring, unique naming, and cleanup verified |
| 2026-08-12 | Step 16 — SQS component tests | Completed | 72 tests passed; API-to-queue attributes and SDK-to-database deletion-after-success flow verified |
| 2026-08-12 | Step 17 — SQS reliability | Completed | 75 tests passed; visibility/redelivery, receive count, redrive, FIFO order, and deduplication verified |
| 2026-08-12 | Step 18 — CI and evidence | Completed | 76 tests passed; workflow structure, split selections, timeouts, JUnit uploads, and full cleanup verified |
| 2026-08-12 | Step 19A — Persistent local infrastructure | Completed | Compose configuration valid; Kafka, Console, LocalStack, PostgreSQL, and Adminer healthy; host SDK connections and browser UIs verified |
| 2026-08-12 | Step 19B — Local application and visual journey | Completed | Browser sent real Kafka and SQS events; consumers committed/deleted after PostgreSQL; pause/resume made queued state observable |
| 2026-08-12 | Step 19 — Final local-lab gate | Completed | 81 tests passed while lab ran; Status/Stop/Start preserved Kafka and SQS database rows; disposable test containers cleaned |
| 2026-08-15 | Step 20 — Package-boundary naming cleanup | Completed | `order_app` contains application/runtime code; `tests/helpers` contains pytest support; 81 tests passed |
| 2026-08-15 | Step 21A — Runtime/test responsibility separation | Completed | Fast gate: 53 tests passed; complete regression: 81 tests passed |
| 2026-08-15 | Step 21B — Responsibility-based file and folder names | Completed | Imports/Compose entry points passed; 81 tests passed; rebuilt visual lab healthy with data preserved |
| 2026-08-15 | Step 21C — Helper and publisher consolidation | Completed | 54 fast tests and 82 total tests passed; rebuilt visual lab healthy with 8 existing orders preserved |

## Decision record

### D-001 — Implement Kafka before SQS

- Status: Accepted
- Decision: Complete the Kafka learning path before adding SQS/LocalStack.
- Reason: Kafka consumer groups and offsets provide the clearest starting model. SQS has different competing-consumer and visibility semantics that should be introduced explicitly afterward.
- Consequence: Phases 1–4 are Kafka-focused; SQS begins in Phase 5.

### D-002 — Use Python 3.12 and pytest

- Status: Accepted
- Decision: Use Python 3.12 as the implementation language and pytest as the runner.
- Reason: The machine already has Python 3.12 and pytest, and the ecosystem supports Kafka, AWS SDK, Testcontainers, fixtures, parametrization, and CI reporting.
- Consequence: The reusable harness and sample system are Python packages under `src/`.

### D-003 — Separate the harness from the sample system

- Status: Accepted
- Decision: Use `mqtest` for reusable test utilities and `sample_app` for the demonstrator producer/consumer application.
- Reason: An SDET framework should be reusable without coupling its clients, probes, polling, contracts, and evidence collection to the sample business service.
- Consequence: Tests may use both packages, but `mqtest` must not import business behavior from `sample_app`.

### D-004 — Use a `src` package layout

- Status: Accepted
- Decision: Place importable packages below `src/` and tests below `tests/`.
- Reason: The layout prevents accidental imports from the repository root and reflects how an installed package behaves.
- Consequence: pytest receives `src` through its configured Python path during the bootstrap step; editable installation remains available for normal development.

### D-005 — Define and enforce test markers early

- Status: Accepted
- Decision: Register `unit`, `contract`, `integration`, `kafka`, `sqs`, and `reliability` markers and enable strict marker validation.
- Reason: The framework will soon contain tests with very different runtime and infrastructure needs. Strict markers prevent spelling mistakes from silently selecting the wrong suite.
- Consequence: Every test added during implementation must be assigned to its appropriate layer.

### D-006 — Keep runtime dependencies empty in Step 1

- Status: Accepted
- Decision: Add only pytest as an optional development dependency in the bootstrap step.
- Reason: Step 1 proves the project foundation without prematurely introducing Kafka, Docker, web, database, or schema libraries.
- Consequence: Kafka/Testcontainers dependencies are added only in Step 2, when they have an executable use.

### D-007 — Pin dependency ranges and container versions

- Status: Accepted
- Decision: Use bounded dependency ranges and explicit container image versions; never use floating `latest` tags.
- Reason: Reproducible asynchronous tests require controlled broker, SDK, and infrastructure behavior.
- Consequence: Every later dependency or container addition must document its selected version in this file.

### D-008 — Use a repository-local virtual environment

- Status: Accepted
- Decision: Use `.venv` inside the repository as the documented local Python environment and exclude it from version control.
- Reason: It isolates project dependencies from global Python packages and makes the setup and verification commands reproducible.
- Consequence: Local commands use the activated environment; verification may invoke `.venv\Scripts\python.exe` explicitly to prove isolation.

### D-009 — Pin Testcontainers and the Kafka client exactly

- Status: Accepted
- Decision: Use `testcontainers[kafka]==4.14.2` and `confluent-kafka==2.15.0` for the Step 2 baseline.
- Reason: Both releases support Python 3.12. Testcontainers community modules may make breaking changes in minor releases, so exact pins are safer for a learning framework. The Confluent client supplies the Producer, Consumer, and AdminClient APIs used in later steps.
- Consequence: Dependency upgrades are deliberate decisions and must be reverified against the infrastructure suite.

### D-010 — Use the Testcontainers-supported Confluent Kafka image in KRaft mode

- Status: Accepted
- Decision: Pin `confluentinc/cp-kafka:7.6.0` and start it through `KafkaContainer.with_kraft()`.
- Reason: Testcontainers Python 4.14.2 explicitly supports this image and lifecycle. KRaft provides a single disposable broker without a separate ZooKeeper container.
- Consequence: The framework tests standard Apache Kafka client semantics through a Confluent distribution. Switching to the official `apache/kafka` image would require a separately verified container adapter.

### D-011 — Fail Kafka integration tests clearly when Docker is unavailable

- Status: Accepted
- Decision: Perform an explicit Docker ping before container startup and fail the infrastructure fixture with recovery guidance.
- Reason: An opaque named-pipe or socket traceback does not teach whether the broker, client, test, or container runtime failed.
- Consequence: Unit tests mock the readiness boundary; integration tests require a running Docker engine and provide a direct “Start Docker Desktop” message otherwise.

### D-012 — Use a unique readable topic for every test

- Status: Accepted
- Decision: Generate topic names from the pytest node ID plus a 12-character random token, normalized to Kafka-safe lowercase characters and capped at 249 characters.
- Reason: Readable names aid diagnosis, while the random suffix prevents collisions across retries, processes, and parallel CI runs.
- Consequence: Tests never depend on a shared hard-coded topic name.

### D-013 — Use three partitions and replication factor one in the learning fixture

- Status: Accepted
- Decision: Provision test topics with three partitions, replication factor one, `cleanup.policy=delete`, and ten-minute retention.
- Reason: Three partitions allow later routing and ordering lessons. The disposable environment contains one broker, so replication factor one is the only valid baseline.
- Consequence: The fixture teaches partition metadata but does not simulate broker-replica failover.

### D-014 — Own topic lifecycle at function scope

- Status: Accepted
- Decision: Create a topic before each test and synchronously verify its deletion during fixture teardown.
- Reason: Per-test ownership prevents retained records or topic metadata from leaking between scenarios.
- Consequence: Topic creation adds integration-test time, but deterministic isolation is preferred for this learning framework.

### D-015 — Treat topic metadata propagation as eventually consistent

- Status: Accepted
- Decision: After Kafka acknowledges topic creation, poll metadata until the requested partition count appears or the administration deadline expires.
- Reason: A repeated integration run demonstrated that the create-topic future can complete just before another metadata request observes the new topic.
- Consequence: Temporary `not found` metadata is retried within a bounded deadline. A regression unit test preserves this behavior, while the final timeout reports the last metadata error.

### D-016 — Commit and push every verified implementation step

- Status: Accepted
- Decision: After a step satisfies all acceptance criteria, update this decision log, create one focused Git commit, and push `main` to `origin` before beginning the next step.
- Reason: Step-level commits make the learning progression reviewable, reversible, and easy to compare.
- Consequence: A step is not considered fully delivered until tests pass, its decision entry is current, and its commit message/reference is recorded. Commit hashes remain available through Git history without creating a self-referential documentation update. Steps 1–3 predate this policy and are captured in one explicit baseline commit.

### D-017 — Use Pydantic for typed models and JSON Schema for the wire contract

- Status: Accepted
- Decision: Pin `pydantic==2.13.4` and `jsonschema[format-nongpl]==4.26.0` as core dependencies.
- Reason: Pydantic gives application code strict typed objects, while Draft 2020-12 JSON Schema provides a language-neutral contract for producers and consumers. Both layers test different boundaries.
- Consequence: The model and schema must remain aligned; contract tests validate model-generated wire payloads against the packaged schema.

### D-018 — Version the contract in both type and schema

- Status: Accepted
- Decision: Define `OrderCreatedEvent` with literal `event_type="order.created"`, literal `event_version=1`, and a packaged `order-created-v1.json` schema.
- Reason: Explicit versions make unsupported changes fail clearly and provide a path for side-by-side evolution.
- Consequence: A future incompatible version receives a new model/schema instead of silently widening version 1.

### D-019 — Reject coercion and unknown fields

- Status: Accepted
- Decision: Configure typed models as strict with `extra="forbid"`, and set `additionalProperties=false` throughout the JSON Schema.
- Reason: Message contracts should expose producer mistakes rather than coerce values such as the string `"500.00"` into a number or ignore misspelled fields.
- Consequence: Contract failures include JSON-style field paths and all schema violations observed in one validation pass.

### D-020 — Enable idempotent, fully acknowledged producer delivery explicitly

- Status: Accepted
- Decision: Configure `enable.idempotence=true`, `acks=all`, a 15-second delivery timeout, a 5-second request timeout, and zero linger for the learning producer.
- Reason: Producer guarantees should be visible in code instead of relying on client defaults. Short bounded timeouts keep test failures actionable, while idempotence protects broker retries from appending duplicates.
- Consequence: These settings strengthen Kafka publication only; they do not make consumer-to-database processing exactly once.

### D-021 — Make one-message test publication synchronous

- Status: Accepted
- Decision: Enqueue with `produce()`, capture its delivery callback, and call bounded `flush()` before returning a `PublishedRecord`.
- Reason: The framework needs deterministic per-event evidence for component tests, not maximum producer throughput.
- Consequence: The wrapper returns topic, partition, offset, timestamp, key, and headers. A production service may later use an asynchronous publishing boundary rather than flushing per request.

### D-022 — Use order ID as key and duplicate contract identity in headers

- Status: Accepted
- Decision: Encode `order_id` as the Kafka key and publish content type, event type/version, event ID, correlation ID, and causation ID as headers.
- Reason: The key provides stable per-order partition routing; headers support tracing and filtering without parsing the payload.
- Consequence: The JSON payload remains the source of truth and the producer constructs both payload and headers from the same typed event.

### D-023 — Make test-layer directories Python packages

- Status: Accepted
- Decision: Add package markers to `tests`, `tests/unit`, `tests/contracts`, and `tests/integration`.
- Reason: Unit and integration layers can then use the same descriptive test filename without pytest importing both as one top-level module.
- Consequence: Test module identity includes its architectural layer, such as `tests.unit.test_kafka_producer`.

### D-024 — Give every probe an independent consumer group

- Status: Accepted
- Decision: Generate a unique `mqtest-probe-<uuid>` group ID for each probe unless a test supplies an explicit diagnostic group.
- Reason: Kafka tracks offsets independently per consumer group. A unique test group can read the producer's records without taking partitions from, or advancing offsets for, the application consumer group.
- Consequence: Producer tests observe rather than steal events. The group ID is included in timeout evidence so failures can be traced.

### D-025 — Start explicitly and never commit probe offsets

- Status: Accepted
- Decision: Subscribe and wait for partition assignment before the producer trigger, set `auto.offset.reset=earliest`, disable automatic commit and offset storage, and read only committed transactional records.
- Reason: The test must know its observation point and must not create misleading offset state. Waiting for assignment removes startup ambiguity, while isolated topics plus `earliest` prevent a publish-before-poll race from hiding the event.
- Consequence: The probe is intentionally not a business-processing consumer. Its context manager always closes the consumer, and every wait has a caller-defined deadline.

### D-026 — Match typed events and retain bounded failure evidence

- Status: Accepted
- Decision: Parse observed values through the versioned `order.created` contract, match with an event/correlation predicate, skip unrelated or malformed records, and retain only the latest ten compact summaries by default.
- Reason: A producer test should find its intended event without failing on normal topic traffic, but a timeout still needs enough evidence to diagnose the group, identities, partitions, and offsets observed.
- Consequence: `correlation_id` locates one test journey; it still does not route the record or provide idempotency. Full payload assertions happen only after a matching typed record is returned.

### D-027 — Use a FastAPI application factory with an injected publisher

- Status: Accepted
- Decision: Build the sample API with FastAPI `0.139.2` and construct it through `create_order_app(publisher, topic)` rather than creating a broker client at module import time.
- Reason: The HTTP mapping can be tested with a recording publisher and no Docker, while the exact same application accepts the real Kafka publisher in a component test. Environment configuration is isolated in a separate runnable factory.
- Consequence: `sample_app` owns the business/API boundary and depends only on the publisher protocol; `mqtest` remains the reusable broker harness.

### D-028 — Return acceptance only after broker acknowledgement

- Status: Accepted
- Decision: Validate a strict request, propagate or generate `X-Correlation-ID`, publish synchronously through the existing reliable wrapper, then return `202 Accepted` with order, correlation, and event IDs.
- Reason: For this learning producer, an accepted response must mean Kafka acknowledged the record. Validation errors must occur before publication, and broker failure must become a sanitized `503` response.
- Consequence: This endpoint favors deterministic evidence over throughput. A production high-throughput API could instead use an outbox pattern, which is outside the current step.

### D-029 — Use the current Starlette HTTPX2 test client path

- Status: Accepted
- Decision: Pin `httpx2==2.9.0` for API tests instead of the deprecated plain-HTTPX compatibility path.
- Reason: Current Starlette documentation identifies HTTPX2 as its maintained `TestClient` dependency. The first successful run with HTTPX emitted a deprecation warning; switching removed it.
- Consequence: API unit/component tests remain synchronous and warning-free. FastAPI, Uvicorn, HTTPX2, and the Kafka runtime client are pinned explicitly.

### D-030 — Keep the producer component boundary in-process but use a real broker

- Status: Accepted
- Decision: Drive the FastAPI ASGI application through its HTTP test client, inject the real `KafkaEventProducer`, and observe a disposable real Kafka topic through `KafkaEventProbe`.
- Reason: The test exercises HTTP parsing, business mapping, serialization, broker acknowledgement, and broker storage without adding port/process management that does not belong to the producer behavior under test.
- Consequence: Uvicorn transport is covered separately by framework confidence; the component failure surface remains focused on API mapping, Kafka publication, and probe observation.

### D-031 — Make negative publication assertions short and isolated

- Status: Accepted
- Decision: For invalid API input, start the probe first, call the API, then assert that no event with the test correlation ID appears within one second on the test-owned topic.
- Reason: “No event” can never be proved with an unbounded wait. A short deadline is meaningful only because the topic and correlation ID belong exclusively to that test.
- Consequence: The negative test expects both HTTP `422` and a probe timeout with zero observed records; it never consumes from shared application state.

### D-032 — Use Psycopg 3 with explicit SQL initialization

- Status: Accepted
- Decision: Pin `psycopg[binary]==3.3.4`, use a pinned `postgres:16.4-alpine` Testcontainer, and initialize the two learning tables with explicit SQL rather than introducing an ORM or migration framework.
- Reason: The component needs visible transaction boundaries and only two tables. Psycopg connection contexts commit on success and roll back on exceptions, making the failure window straightforward to teach and test.
- Consequence: `orders` stores the business state and source identifiers; `processed_events` stores event identity. A production schema migration tool remains a later operational concern.

### D-033 — Commit Kafka only after the database transaction succeeds

- Status: Accepted
- Decision: Disable Kafka auto-commit and auto-offset-store, persist the order and processed event in one PostgreSQL transaction, then call `commit(message=..., asynchronous=False)`.
- Reason: Committing the offset first could permanently lose the business update after a crash. Database-first ordering gives at-least-once delivery, so a crash after database commit but before offset commit may redeliver and requires idempotency.
- Consequence: Step 9 proves rollback leaves the offset uncommitted by restarting the same group and receiving the record again. Step 12 will make the post-database/pre-offset duplicate window safe.

### D-034 — Isolate PostgreSQL state by schema per test

- Status: Accepted
- Decision: Reuse one session PostgreSQL container but create a UUID-named schema for each test and drop it afterward with quoted SQL identifiers.
- Reason: Function-owned schemas prevent cross-test data collisions while avoiding the startup cost of a database container per case.
- Consequence: Integration evidence includes the schema name, and cleanup removes all test tables even after a failure.

### D-035 — Centralize bounded polling in `eventually`

- Status: Accepted
- Decision: Implement one generic observer/predicate helper using a monotonic deadline, immediate first observation, configurable polling interval, and a structured `EventuallyTimeout` assertion.
- Reason: Asynchronous tests should finish as soon as state appears and should fail with the last real evidence instead of waiting a fixed duration or looping forever.
- Consequence: Timeout evidence exposes description, attempt count, elapsed seconds, and last observed value. Test code contains no fixed `sleep()` synchronization.

### D-036 — Run the consumer independently from state observation

- Status: Accepted
- Decision: Execute the blocking one-record consumer on a worker thread, publish through the SDK, and poll PostgreSQL from the test thread until the business row exists.
- Reason: This matches the real asynchronous relationship while keeping the consumer implementation deterministic and avoiding a permanent service process in component tests.
- Consequence: The test separately awaits the consumer result to prove the post-database Kafka commit also completed; database visibility alone is not treated as offset-commit evidence.

### D-037 — Use an owned in-process real-HTTP recording stub

- Status: Accepted
- Decision: Build a context-managed `ThreadingHTTPServer` stub that binds an ephemeral loopback port, queues response status/body pairs, and records immutable request evidence.
- Reason: The scenario needs real TCP/HTTP serialization but not a separate container product. An owned stub is fast, deterministic, dependency-free, and sufficient for method/path/header/body verification.
- Consequence: Every test owns its server lifecycle and response script. More advanced service virtualization can be introduced later without changing the consumer adapter contract.

### D-038 — Treat downstream acceptance as part of successful event handling

- Status: Accepted
- Decision: Call the downstream adapter after the database store and before Kafka offset commit. Propagate event/correlation IDs as headers and retry only up to an explicitly configured attempt count.
- Reason: Kafka must not advance when a required downstream effect is rejected. A small adapter-level attempt count demonstrates a temporary `503` followed by acceptance; Step 13 will define the complete retry/backoff/DLQ policy.
- Consequence: Exhausted HTTP attempts raise a consumer error and leave the Kafka offset uncommitted. The database-to-offset crash window still requires Step 12 idempotency.

### D-039 — Track event processing as pending then completed

- Status: Accepted
- Decision: Atomically claim `event_id` as `pending` and insert the order in one transaction. After required downstream work succeeds, update the marker to `completed`; only then commit Kafka.
- Reason: A single “processed” flag written before the HTTP effect could suppress unfinished work after redelivery. Pending/completed state distinguishes a completed duplicate from a crash-window retry.
- Consequence: A completed duplicate skips database and HTTP effects but commits its own offset. A pending duplicate retries the downstream effect without inserting another order, then completes the marker.

### D-040 — Deduplicate only by event ID

- Status: Accepted
- Decision: Use the event ID primary key as the idempotency identity; never use correlation ID for duplicate suppression.
- Reason: Correlation ID describes a journey and is intentionally shared by multiple legitimate events. Event ID identifies one immutable event delivery.
- Consequence: Two different event IDs with the same correlation ID create two valid orders and downstream requests; publishing the same event ID twice creates one of each effect.

### D-041 — Make retries classified and bounded

- Status: Accepted
- Decision: Configure maximum attempts, backoff, and an explicit tuple of retryable exception types. Undeclared exceptions are non-retryable; all retry loops are finite.
- Reason: Retrying validation or permanent business failures wastes resources and can block a partition. Deterministic attempts make behavior and tests explainable.
- Consequence: Consumer-level retries re-enter the idempotent pending-event flow. Tests use zero backoff for speed; production-like configurations can use a positive bounded value.

### D-042 — Dead-letter only after cleanup and broker acknowledgement

- Status: Accepted
- Decision: After retry exhaustion, atomically discard the event's pending order/marker, publish a broker-acknowledged DLQ envelope, then commit the original poison offset.
- Reason: This leaves no partial business state and lets the partition continue. If DLQ publication fails, the original offset is not committed and Kafka can redeliver the source event.
- Consequence: DLQ evidence includes source topic/partition/offset/key, event/correlation IDs, attempts, error type/message, timestamp, and original payload. A later valid same-key record is processed after the poison record.

### D-043 — Scope Kafka ordering assertions to a partition

- Status: Accepted
- Decision: Assert that identical keys select one partition and offsets increase there. For different keys, group evidence by partition and never compare offsets across partitions.
- Reason: Kafka ordering and offsets are partition-local; presenting a topic-wide sequence would teach a false guarantee.
- Consequence: Same-key order is directly verified, while the cross-key test proves distribution and only partition-local monotonicity.

### D-044 — Configure transaction state for the single test broker

- Status: Accepted
- Decision: Set transaction-state replication factor and minimum ISR to one only on the disposable single-broker Kafka fixture.
- Reason: Kafka's production-oriented transaction-state defaults expect multiple brokers and caused coordinator initialization to time out in the focused test.
- Consequence: Transactional tests work on the learning broker; this configuration is explicitly unsuitable as a production availability recommendation.

### D-045 — Use pinned LocalStack and fake local AWS identity

- Status: Accepted
- Decision: Run `localstack/localstack:3.5.0` through Testcontainers with only SQS enabled and use the container-created boto3 client with non-production credentials/endpoint.
- Reason: SQS semantics require a real compatible service API without storing cloud credentials or creating paid/shared resources.
- Consequence: Local tests never contact AWS. Boto3 is pinned as the production SQS SDK adapter and LocalStack remains a development dependency.

### D-046 — Give every SQS test its own queue family

- Status: Accepted
- Decision: Provision function-scoped standard, FIFO, and DLQ queues with two-second visibility, one-second long polling, standard-to-DLQ redrive after three receives, and explicit FIFO deduplication settings.
- Reason: Unlike Kafka consumer groups, receiving from SQS hides a message from competitors. An observer test must own the queue it receives from.
- Consequence: Queue URLs appear in JUnit evidence and all three queues are deleted after each test. Naming always preserves hash/random suffixes under AWS's 80-character limit.

### D-047 — Keep SQS adapter vocabulary queue-native

- Status: Accepted
- Decision: Expose queue URL, message ID, receipt handle, message attributes, receive attributes, message group, and deduplication ID; do not expose Kafka partition/group/offset concepts.
- Reason: A common event contract does not make broker acknowledgement and competition semantics interchangeable.
- Consequence: The SQS publisher returns AWS message evidence and the owned-queue probe long-polls to a deadline. It deletes observed messages only because the test owns that queue.

### D-048 — Delete SQS messages only after successful effects

- Status: Accepted
- Decision: The SQS consumer validates and persists the event, completes its idempotency marker, then deletes the exact receipt handle.
- Reason: Delete is SQS acknowledgement. Deleting before the database transaction could lose the event; not deleting after failure lets visibility expiry make it eligible for redelivery.
- Consequence: The component test proves database state exists and the queue is empty after success. Step 17 directly tests the no-delete redelivery path.

### D-049 — Test SQS delivery controls without calling them idempotency

- Status: Accepted
- Decision: Verify visibility, receive count, redrive, FIFO message-group order, and FIFO deduplication ID as distinct queue behaviors.
- Reason: Visibility controls competition, deletion acknowledges, redrive isolates poison messages, FIFO groups scope ordering, and deduplication suppresses sends only within its supported window. None alone makes business side effects idempotent.
- Consequence: Tests make each mechanism observable and still retain database event-ID idempotency in the consumer design.

### D-050 — Gate Docker suites behind fast feedback

- Status: Accepted
- Decision: Run unit/contract first, then launch independent Kafka and SQS integration jobs only after it succeeds.
- Reason: Contract and mapping errors should fail quickly without spending time or runner capacity on containers.
- Consequence: Kafka and SQS remain independently diagnosable while sharing the same installed project and Python 3.12 baseline.

### D-051 — Always retain bounded CI evidence

- Status: Accepted
- Decision: Apply ten-minute fast and twenty-minute infrastructure job timeouts, create per-suite JUnit XML, and upload it with `if: always()`.
- Reason: A timed-out or failed asynchronous test needs identifiers and infrastructure evidence after the runner is gone.
- Consequence: A unit test parses the workflow and guards the three jobs, dependencies, timeout ceiling, and artifact upload steps.

### D-052 — Keep the visual lab separate from disposable tests

- Status: Accepted
- Decision: Add a persistent `compose.local.yml` learning environment without changing the Testcontainers fixtures used by automated tests.
- Reason: Visual exploration needs stable ports and retained data, while trustworthy automated tests need isolated resources and deterministic cleanup.
- Consequence: Local lab services live until explicitly stopped; the existing automated-test baseline remains disposable and CI-safe.

### D-053 — Bind development ports to IPv4 loopback

- Status: Accepted
- Decision: Expose local-lab services only on `127.0.0.1` and advertise Kafka externally as `127.0.0.1:29092`.
- Reason: Loopback binding avoids exposing unsecured learning services to the LAN and avoids a Windows IPv6 `localhost` connection delay observed during verification.
- Consequence: Browser and SDK documentation uses explicit `127.0.0.1` addresses; containers communicate through their internal Compose network names.

### D-054 — Reuse tested runtime code in continuous local workers

- Status: Accepted
- Decision: Package the existing API publisher, contract parsing, Kafka/SQS consumers, and PostgreSQL store into one local application image rather than building demonstration-only duplicates.
- Reason: The visual journey must teach the same behavior the automated component tests verify.
- Consequence: Kafka still commits after effects and SQS still deletes after effects; stable resources change lifecycle only, not business semantics.

### D-055 — Make broker waiting state controllable and visible

- Status: Accepted
- Decision: Store Kafka and SQS consumer pause state in a small PostgreSQL control table and expose pause/resume buttons in the dashboard.
- Reason: A continuously running SQS consumer can receive and delete a successful message before a learner sees it waiting in the queue.
- Consequence: A learner can pause, publish, observe broker acknowledgement without a database effect, then resume and observe acknowledgement completion. The control is local-lab-only and is not part of production sample logic.

### D-056 — Name runtime code and test helpers by responsibility

- Status: Accepted; supersedes the package naming portion of D-003.
- Decision: Rename `sample_app` to `order_app`, place runtime contracts and Kafka/SQS clients under `order_app.messaging`, and move test-only polling, HTTP stubs, and infrastructure helpers under `tests.helpers`.
- Reason: `sample_app` did not identify the business domain, while `mqtest` and the proposed `order_app_tests` obscured the difference between reusable support code and the actual pytest cases already stored under `tests`.
- Consequence: Application code never imports from `tests`; actual test cases remain under `tests/unit`, `tests/contracts`, and `tests/integration`; reusable test-only code has the explicit `tests/helpers` boundary.

### D-057 — Do not package test probes and test resource factories as application code

- Status: Accepted
- Decision: Keep production-capable publishers and consumers under `src/order_app`, but move broker observation probes, generated test resource names, and pinned Testcontainers image names to `tests/helpers`.
- Reason: These objects are needed by automated tests, but the running order application never calls them. Packaging them beside runtime clients made unused test machinery appear to be part of the application.
- Consequence: `SqsEventClient` is reduced to publishing only; the SQS receive-and-assert behavior is named `SqsQueueProbe` under test helpers. Kafka/SQS resource factories and all container image pins also live under the test boundary.

### D-058 — Name modules for the responsibility visible in the repository tree

- Status: Accepted
- Decision: Replace context-free module names such as `app.py`, `client.py`, `store.py`, `workers.py`, and `infrastructure.py` with responsibility names such as `http_api.py`, `event_publisher.py`, `postgres_order_store.py`, `consumer_worker.py`, and `resource_setup.py`.
- Reason: A learner should understand the likely purpose of a file before opening it. Generic names were technically valid but made runtime wiring and duplicate-looking concepts difficult to distinguish.
- Consequence: The business-processing package is `order_processing`; one-file test-helper subpackages are flattened; `docs/code-map.md` records every runtime and helper file, its purpose, and its caller. Narrow internal names such as `contracts/models.py` remain because their parent folder supplies the missing context.

### D-059 — Consolidate by domain and require every class to justify itself

- Status: Accepted; supersedes the helper-file layout in D-057 and publisher layout in D-058.
- Decision: Keep exactly four implementation modules under `tests/helpers` (`kafka.py`, `sqs.py`, `client_stub.py`, and `docker.py`). Put both broker publishers in `messaging/event_publishers.py` behind the generic `EventPublisher` base class.
- Reason: Splitting a single test domain across resource-name, probe, image, and readiness modules added navigation cost without creating useful boundaries. Kafka and SQS publishers perform the same application role and are clearer as child implementations of one explicit contract.
- Consequence: Stateless SQS resource/probe wrappers become functions. Classes remain only for state/lifecycle, structured data, polymorphic behavior, or distinct exception handling. The required `tests/helpers/__init__.py` package marker contains no helper implementation.

## Verification log

### Step 1 — Python project bootstrap

- Date: 2026-08-12
- Environment: Windows, Python 3.12.2, clean repository-local `.venv`
- Installed project: editable `mqtest-learning-framework==0.1.0` with the `dev` extra
- Resolved pytest: 8.4.2, within the declared `>=8.2,<9` range
- Command: `.\.venv\Scripts\python.exe -m pytest -m unit`
- Result: Passed — 1 test collected, 1 passed
- Collection command: `.\.venv\Scripts\python.exe -m pytest --collect-only -q`
- Collection result: Passed — exactly 1 test discovered
- Acceptance criteria:
  - Fresh virtual environment installs the project: Passed
  - Unit marker runs without Docker: Passed
  - Harness and sample application are separate importable packages: Passed
  - No broker/application behavior introduced: Passed
- Note: An initial sandboxed isolated-build dry run could not reach the package index. The approved installation into `.venv` subsequently downloaded the declared build/test dependencies and completed successfully.

### Step 2 — Disposable Kafka

- Date: 2026-08-12
- Environment: Windows, Python 3.12.2, Docker Engine 29.6.2
- Dependencies: `testcontainers[kafka]==4.14.2`, `confluent-kafka==2.15.0`
- Broker image: `confluentinc/cp-kafka:7.6.0`, KRaft mode
- Negative-path command: `.\.venv\Scripts\python.exe -m pytest -m "integration and kafka" -q` with Docker stopped
- Negative-path result: Expected infrastructure error with explicit instruction to start Docker Desktop
- Positive-path command: `.\.venv\Scripts\python.exe -m pytest -m "integration and kafka" -q`
- Positive-path result: Passed — 1 Kafka integration test passed
- Full-suite command: `.\.venv\Scripts\python.exe -m pytest`
- Full-suite result: Passed — 4 tests collected, 4 passed (final run: 15.31 seconds)
- Dependency check: `.\.venv\Scripts\python.exe -m pip check` passed with no broken requirements
- Cleanup check: `docker ps --filter "ancestor=confluentinc/cp-kafka:7.6.0"` returned no running broker container after the suite
- Acceptance criteria:
  - Kafka starts automatically for the integration suite: Passed
  - Fixture exposes a working bootstrap-server address: Passed
  - AdminClient discovers broker metadata: Passed
  - Kafka container is removed after the suite: Passed
  - Docker-unavailable failure provides recovery guidance: Passed
  - Unit suite remains independent of Docker: Passed

### Step 3 — Isolated Kafka topics

- Date: 2026-08-12
- Unit command: `.\.venv\Scripts\python.exe -m pytest -m unit`
- Unit result: Passed — 7 selected tests passed
- Kafka command: `.\.venv\Scripts\python.exe -m pytest -m "integration and kafka" -vv`
- Kafka result: Passed — connectivity and two topic lifecycle tests passed
- Final command: `.\.venv\Scripts\python.exe -m pytest --junitxml="test-results\step3.xml"`
- Final result: Passed — 10 tests collected, 10 passed in 19.06 seconds
- Evidence result: JUnit contains `kafka_topic`, `kafka_partitions=3`, replication factors, and requested topic configuration
- Cleanup result: Explicit lifecycle assertion proved deleted topic metadata disappeared; no Kafka broker container remained after the suite
- Reliability finding: A repeated run caught topic creation acknowledgement preceding metadata visibility. D-015 and a unit regression test now enforce bounded metadata polling.
- Acceptance criteria:
  - Parallel-safe unique topic names: Passed
  - Topic becomes ready before fixture yield: Passed
  - Three partitions and replication factor one are asserted: Passed
  - Requested topic configuration is captured as test evidence: Passed
  - Topic deletion is synchronously verified: Passed
  - Full suite and infrastructure cleanup pass: Passed

### Step 4 — Versioned event contract

- Date: 2026-08-12
- Dependencies: `pydantic==2.13.4`, `jsonschema[format-nongpl]==4.26.0`
- Contract suite command: `.\.venv\Scripts\python.exe -m pytest -m "unit or contract" -vv`
- Contract suite result: Passed — 14 selected tests passed
- Dependency command: `.\.venv\Scripts\python.exe -m pip check`
- Dependency result: Passed — no broken requirements
- Full-suite command: `.\.venv\Scripts\python.exe -m pytest --junitxml="test-results\step4.xml"`
- Full-suite result: Passed — 17 tests collected, 17 passed in 19.60 seconds
- Package command: `.\.venv\Scripts\python.exe -m pip wheel --no-deps --wheel-dir "test-results" .`
- Package result: Passed — wheel built through isolated PEP 517 build; `mqtest/contracts/schemas/order-created-v1.json` is included
- Cleanup result: No Kafka broker container remained after the full suite
- Packaging note: A diagnostic `--no-build-isolation` attempt failed because the virtual environment intentionally did not contain the build-only `setuptools` requirement. The declared isolated build installed it and succeeded; runtime dependencies were unaffected.
- Acceptance criteria:
  - Valid typed events serialize and satisfy JSON Schema: Passed
  - Missing required fields report JSON field paths: Passed
  - Wrong nested types report the affected field: Passed
  - Unsupported event versions are rejected: Passed
  - UUID and date-time formats are actively checked: Passed
  - Event, correlation, and causation identity roles are demonstrated: Passed
  - Packaged distribution includes the schema: Passed

### Step 5 — Kafka producer client

- Date: 2026-08-12
- Unit/contract command: `.\.venv\Scripts\python.exe -m pytest -m "unit or contract" -vv`
- Unit/contract result: Passed — 19 selected tests passed
- Kafka command: `.\.venv\Scripts\python.exe -m pytest -m "integration and kafka" -vv --junitxml="test-results\step5-kafka.xml"`
- Kafka result: Passed — 4 selected integration tests passed
- Delivery evidence: JUnit includes event ID, correlation ID, Kafka partition, and Kafka offset
- Final command: `.\.venv\Scripts\python.exe -m pytest`
- Final result: Passed — 23 tests collected, 23 passed in 19.77 seconds
- Dependency result: `pip check` passed with no broken requirements
- Cleanup result: No Kafka broker container remained after either integration run
- Implementation finding: Matching unit and integration filenames collided when test layers were not Python packages. D-023 makes module identity layer-specific.
- Acceptance criteria:
  - Contract-valid event is acknowledged by real Kafka: Passed
  - Idempotence and `acks=all` are explicit: Passed
  - Order ID is encoded as the Kafka key: Passed
  - Contract/tracing metadata is encoded as Kafka headers: Passed
  - Delivery callback failure becomes a diagnostic exception: Passed
  - Bounded flush reports undelivered count: Passed
  - Returned evidence contains topic, partition, offset, timestamp, key, and headers: Passed

### Step 6 — Kafka test probe

- Date: 2026-08-12
- Unit/contract command: `.\.venv\Scripts\python.exe -m pytest -m "unit or contract" -vv`
- Unit/contract result: Passed — 24 selected tests passed
- Focused Kafka command: `.\.venv\Scripts\python.exe -m pytest tests/integration/test_kafka_probe.py -vv --junitxml="test-results\step6-kafka.xml"`
- Focused Kafka result: Passed — 2 probe integration tests passed in 8.90 seconds
- Final command: `.\.venv\Scripts\python.exe -m pytest --junitxml="test-results\step6-full.xml"`
- Final result: Passed — 30 tests collected, 30 passed in 11.87 seconds
- Dependency result: `pip check` passed with no broken requirements
- Cleanup result: No Kafka broker container remained after verification
- Environment note: The first focused run was correctly blocked by restricted Docker named-pipe access; the approved Docker-enabled run passed without code changes.
- Evidence result: JUnit includes probe group ID, event ID, correlation ID, Kafka partition, and Kafka offset
- Acceptance criteria:
  - Probe receives partition assignment before the trigger: Passed
  - Unique group observes independently with commits disabled: Passed
  - Unrelated and malformed records do not create a false match: Passed
  - Event ID and/or correlation ID predicates return the intended typed event: Passed
  - Missing match fails at its deadline with group and observed-record evidence: Passed
  - Context-managed consumer closes on completion: Passed

### Step 7 — Sample producer API

- Date: 2026-08-12
- Framework/runtime: FastAPI `0.139.2`, Uvicorn `0.51.0`, HTTPX2 `2.9.0`
- Focused command: `.\.venv\Scripts\python.exe -m pytest tests/unit/test_order_api.py -vv`
- Focused result: Passed — 7 API unit tests passed
- Final command: `.\.venv\Scripts\python.exe -m pytest --junitxml="test-results\step7-full.xml"`
- Final result: Passed — 37 tests collected, 37 passed in 27.86 seconds
- Dependency result: Editable development installation completed; `pip check` passed
- Cleanup result: No Kafka broker container remained after verification
- Implementation finding: Plain HTTPX still worked but emitted Starlette's deprecation warning. D-029 records the warning-free HTTPX2 choice recommended by current Starlette documentation.
- Acceptance criteria:
  - Valid request returns `202 Accepted` after the publisher succeeds: Passed
  - Supplied correlation ID is propagated to response header/body and event: Passed
  - Missing correlation ID is generated and returned: Passed
  - Request maps exactly to the typed `order.created` event: Passed
  - Missing, invalid, or extra request fields never invoke the publisher: Passed
  - Kafka publication failure maps to a sanitized `503`: Passed

### Step 8 — Kafka producer component test

- Date: 2026-08-12
- Focused command: `.\.venv\Scripts\python.exe -m pytest tests/integration/test_order_producer_component.py -vv --junitxml="test-results\step8-component.xml"`
- Focused result: Passed — 2 component tests passed in 11.86 seconds
- Final command: `.\.venv\Scripts\python.exe -m pytest --junitxml="test-results\step8-full.xml"`
- Final result: Passed — 39 tests collected, 39 passed in 17.85 seconds
- Dependency result: `pip check` passed with no broken requirements
- Cleanup result: No Kafka broker container remained after verification
- Evidence result: JUnit contains probe group, event/correlation IDs, topic, partition, and offset
- Acceptance criteria:
  - Positive test proves HTTP request through real broker record: Passed
  - Destination, key, headers, schema, identifiers, and business payload are asserted: Passed
  - No business consumer or database is running in the producer test: Passed
  - Invalid request returns `422` and produces no matching event: Passed
  - Negative observation is bounded to one second on an isolated topic: Passed

### Step 9 — Disposable PostgreSQL and sample consumer

- Date: 2026-08-12
- Infrastructure/runtime: `postgres:16.4-alpine`, Psycopg `3.3.4`, Testcontainers `4.14.2`
- Unit/contract command: `.\.venv\Scripts\python.exe -m pytest -m "unit or contract" -vv`
- Unit/contract result: Passed — 34 selected tests passed
- Focused command: `.\.venv\Scripts\python.exe -m pytest tests/integration/test_order_consumer.py -vv --junitxml="test-results\step9-consumer.xml"`
- Focused result: Passed — 2 Kafka/PostgreSQL tests passed in 33.36 seconds
- Final command: `.\.venv\Scripts\python.exe -m pytest --junitxml="test-results\step9-full.xml"`
- Final result: Passed — 44 tests collected, 44 passed in 26.11 seconds
- Dependency result: Editable installation and `pip check` passed
- Cleanup result: No Kafka or PostgreSQL test container remained after verification
- Evidence result: JUnit contains consumer group, event/correlation IDs, partition, offset, and isolated PostgreSQL schema
- Acceptance criteria:
  - SDK-published valid event creates exactly one complete order row: Passed
  - Processed event identity is written in the same database transaction: Passed
  - Consumer auto-commit and auto-offset-store are disabled: Passed
  - Kafka offset commit occurs synchronously after database success: Passed
  - Forced second-insert failure rolls back the first insert: Passed
  - Same consumer group receives the rolled-back event again after restart: Passed

### Step 10 — Eventual assertions and consumer component test

- Date: 2026-08-12
- Helper command: `.\.venv\Scripts\python.exe -m pytest tests/unit/test_eventually.py -vv`
- Helper result: Passed — 5 unit tests passed
- Focused command: `.\.venv\Scripts\python.exe -m pytest tests/integration/test_order_consumer_eventually.py -vv --junitxml="test-results\step10-consumer.xml"`
- Focused result: Passed — asynchronous consumer component test passed in 12.89 seconds
- Final command: `.\.venv\Scripts\python.exe -m pytest --junitxml="test-results\step10-full.xml"`
- Final result: Passed — 50 tests collected, 50 passed in 25.40 seconds
- Dependency result: `pip check` passed with no broken requirements
- Cleanup result: No Kafka or PostgreSQL test container remained after verification
- Source scan result: No test contains `time.sleep()` or imports `sleep` for synchronization
- Acceptance criteria:
  - First matching observation returns without an initial delay: Passed
  - Later matching observation is retried and returned: Passed
  - Missing state fails within its deadline: Passed
  - Failure includes attempts, elapsed time, and last value: Passed
  - SDK event is processed asynchronously into complete PostgreSQL state: Passed
  - Consumer completion separately proves offset commit completed: Passed

### Step 11 — Downstream HTTP verification

- Date: 2026-08-12
- Unit/contract command: `.\.venv\Scripts\python.exe -m pytest -m "unit or contract" -q`
- Unit/contract result: Passed — 40 selected tests passed
- Focused command: `.\.venv\Scripts\python.exe -m pytest tests/integration/test_order_consumer_http.py -vv --junitxml="test-results\step11-http.xml"`
- Focused result: Passed — 2 downstream HTTP integration tests passed in 17.90 seconds
- Final command: `.\.venv\Scripts\python.exe -m pytest --junitxml="test-results\step11-full.xml"`
- Final result: Passed — 53 tests collected, 53 passed in 31.41 seconds
- Dependency result: HTTPX2 moved to runtime dependencies; `pip check` passed
- Cleanup result: HTTP stub, Kafka, and PostgreSQL resources were all stopped after verification
- Evidence result: JUnit includes consumer group, event/correlation IDs, and downstream path
- Acceptance criteria:
  - Real downstream request uses expected method and path: Passed
  - Content type, correlation ID, and event ID headers are asserted: Passed
  - JSON body contains exact order business fields: Passed
  - Configurable stub returns success and failure responses: Passed
  - Temporary `503` followed by `202` preserves identity and succeeds: Passed
  - Downstream failure prevents Kafka offset commit in unit orchestration: Passed

### Step 12 — Consumer idempotency

- Date: 2026-08-12
- Unit/contract command: `.\.venv\Scripts\python.exe -m pytest -m "unit or contract" -q`
- Unit/contract result: Passed — 41 selected tests passed
- Focused command: `.\.venv\Scripts\python.exe -m pytest tests/integration/test_order_consumer_idempotency.py -vv --junitxml="test-results\step12-idempotency.xml"`
- Focused result: Passed — 3 reliability tests passed in 19.85 seconds
- Final command: `.\.venv\Scripts\python.exe -m pytest --junitxml="test-results\step12-full.xml"`
- Final result: Passed — 57 tests collected, 57 passed in 39.43 seconds
- Dependency result: `pip check` passed with no broken requirements
- Cleanup result: HTTP stubs, Kafka, and PostgreSQL were cleaned after verification
- Acceptance criteria:
  - Event claim and order insert occur in one database transaction: Passed
  - Same event ID delivered twice creates one order: Passed
  - Completed duplicate does not repeat downstream HTTP: Passed
  - Pending redelivery retries unfinished downstream work: Passed
  - Pending recovery does not insert another order: Passed
  - Different event IDs with the same correlation ID are both processed: Passed

### Step 13 — Retry and DLQ behavior

- Date: 2026-08-12
- Unit/contract command: `.\.venv\Scripts\python.exe -m pytest -m "unit or contract" -q`
- Unit/contract result: Passed — 44 selected tests passed
- Focused command: `.\.venv\Scripts\python.exe -m pytest tests/integration/test_order_consumer_retry_dlq.py -vv --junitxml="test-results\step13-retry-dlq.xml"`
- Focused result: Passed — 2 reliability integration tests passed in 19.42 seconds
- Final command: `.\.venv\Scripts\python.exe -m pytest --junitxml="test-results\step13-full.xml"`
- Final result: Passed — 62 tests collected, 62 passed in 43.67 seconds
- Dependency result: `pip check` passed with no broken requirements
- Cleanup result: Kafka topics/DLQ, PostgreSQL schemas, HTTP stubs, and containers were cleaned
- Acceptance criteria:
  - Retryable and non-retryable exceptions are explicitly classified: Passed
  - Attempt count and backoff are validated and bounded: Passed
  - Temporary HTTP failure succeeds on the configured attempt: Passed
  - Always-failing poison event exhausts exactly three attempts: Passed
  - DLQ record contains original identifiers, source metadata, error, and payload: Passed
  - Poison partial database state is removed: Passed
  - Poison source offset is committed only after DLQ acknowledgement: Passed
  - Later same-key valid event continues and creates the only business row: Passed

### Step 14 — Kafka ordering, recovery, and transactions

- Date: 2026-08-12
- Focused command: `.\.venv\Scripts\python.exe -m pytest tests/integration/test_kafka_ordering_recovery_transactions.py -vv --junitxml="test-results\step14-kafka.xml"`
- Focused result: Passed — 4 reliability tests passed in 26.03 seconds
- Final command: `.\.venv\Scripts\python.exe -m pytest --junitxml="test-results\step14-full.xml"`
- Final result: Passed — 66 tests collected, 66 passed in 51.56 seconds
- Dependency result: `pip check` passed
- Cleanup result: Kafka and PostgreSQL containers/resources were cleaned
- Implementation finding: Initial transaction coordinator setup timed out because the one-broker fixture inherited multi-broker transaction-state defaults. D-044 records the isolated test fix.
- Acceptance criteria:
  - Same-key events use one partition and increasing offsets: Passed
  - Probe observes same-key events in publication order: Passed
  - Different-key ordering assertions remain partition-scoped: Passed
  - Same consumer group resumes after its committed record on restart: Passed
  - `read_committed` sees committed transactional record: Passed
  - `read_committed` excludes aborted transactional record: Passed

### Step 15 — LocalStack and isolated SQS queues

- Date: 2026-08-12
- Infrastructure/runtime: `localstack/localstack:3.5.0`, boto3 `1.43.53`
- Focused command: `.\.venv\Scripts\python.exe -m pytest tests/integration/test_sqs_resources.py -vv --junitxml="test-results\step15-sqs.xml"`
- Focused result: Passed — SQS resource test passed in 6.91 seconds
- Final command: `.\.venv\Scripts\python.exe -m pytest --junitxml="test-results\step15-full.xml"`
- Final result: Passed — 70 tests collected, 70 passed in 55.92 seconds
- Dependency result: Editable install and `pip check` passed
- Cleanup result: LocalStack, Kafka, and PostgreSQL containers/resources were cleaned
- Implementation finding: Initial long resource names truncated away role/unique suffixes and collided. The generator now truncates only the readable middle and unit tests preserve identity.
- Known upstream warning: Testcontainers `4.14.2` LocalStack module uses its deprecated internal `wait_for_logs` helper; this does not affect queue behavior or cleanup.
- Acceptance criteria:
  - Pinned LocalStack starts automatically with SQS: Passed
  - Each test owns standard, FIFO, and DLQ queues: Passed
  - Visibility and receive long-poll attributes are explicit: Passed
  - Standard queue redrive targets its owned DLQ after three receives: Passed
  - FIFO queue type and deduplication mode are explicit: Passed
  - boto3 lists all created queue URLs: Passed
  - Queue family and container are cleaned: Passed

### Step 16 — SQS producer and consumer component tests

- Date: 2026-08-12
- Focused command: `.\.venv\Scripts\python.exe -m pytest tests/integration/test_sqs_components.py -vv --junitxml="test-results\step16-sqs.xml"`
- Focused result: Passed — 2 SQS component tests passed in 10.44 seconds
- Final command: `.\.venv\Scripts\python.exe -m pytest --junitxml="test-results\step16-full.xml"`
- Final result: Passed — 72 tests collected, 72 passed in 53.99 seconds
- Dependency result: `pip check` passed
- Cleanup result: LocalStack, Kafka, PostgreSQL, queue, topic, and schema resources were cleaned
- Acceptance criteria:
  - API publishes typed event through boto3 to an owned queue: Passed
  - Event identity/contract metadata maps to SQS message attributes: Passed
  - Owned-queue probe long-polls and matches correlation identity: Passed
  - SDK event creates complete PostgreSQL consumer state: Passed
  - Receipt handle is deleted only after successful state completion: Passed
  - Queue is empty after successful consumer acknowledgement: Passed

### Step 17 — SQS reliability scenarios

- Date: 2026-08-12
- Focused command: `.\.venv\Scripts\python.exe -m pytest tests/integration/test_sqs_reliability.py -vv --junitxml="test-results\step17-sqs.xml"`
- Focused result: Passed — 3 SQS reliability tests passed in 10.71 seconds
- Final command: `.\.venv\Scripts\python.exe -m pytest --junitxml="test-results\step17-full.xml"`
- Final result: Passed — 75 tests collected, 75 passed in 59.60 seconds
- Dependency result: `pip check` passed
- Cleanup result: All LocalStack/Kafka/PostgreSQL containers and owned resources were cleaned
- Acceptance criteria:
  - Receive without delete hides message during visibility timeout: Passed
  - Message reappears with increased approximate receive count: Passed
  - Repeated poison receives redrive to the owned DLQ: Passed
  - FIFO order is preserved within one message group: Passed
  - Reused FIFO deduplication ID suppresses the duplicate send: Passed
  - Received messages are explicitly deleted during test-owned cleanup: Passed

### Step 18 — Continuous integration and evidence

- Date: 2026-08-12
- CI workflow: `.github/workflows/test.yml`
- Fast command: `.\.venv\Scripts\python.exe -m pytest -m "unit or contract" --junitxml="test-results\step18-fast.xml"`
- Fast result: Passed — 48 selected tests passed in 3.49 seconds
- Kafka collection gate: `integration and kafka` selects 22 tests
- SQS collection gate: `integration and sqs` selects 6 tests
- Final command: `.\.venv\Scripts\python.exe -m pytest --junitxml="test-results\step18-full.xml"`
- Final result: Passed — 76 tests collected, 76 passed in 60.09 seconds
- Dependency result: Editable install with PyYAML `6.0.3`; `pip check` passed
- Cleanup result: No Kafka, PostgreSQL, or LocalStack container remained
- Acceptance criteria:
  - Fast unit/contract job runs without Docker: Passed
  - Kafka and SQS integration jobs are separate and depend on fast gate: Passed
  - All jobs use Python 3.12 and dependency caching: Passed
  - Every job has a hard timeout no greater than 20 minutes: Passed
  - Every job uploads JUnit evidence even after failure: Passed
  - Workflow structure has a fast unit regression guard: Passed
  - Complete clean local baseline passes: Passed

### Step 19A — Persistent local infrastructure

- Date: 2026-08-12
- Compose file: `compose.local.yml`
- Configuration command: `docker compose -f compose.local.yml config --quiet`
- Startup command: `docker compose -f compose.local.yml up -d --wait kafka kafka-console postgres adminer localstack`
- Service result: Kafka, Redpanda Console, LocalStack SQS, PostgreSQL, and Adminer all healthy
- Host connectivity: Confluent AdminClient reached broker 1; boto3 listed SQS; Psycopg connected to database `mqtest`
- Browser result: Redpanda Console reported one online Kafka broker; Adminer exposed its PostgreSQL login UI
- Implementation finding: Windows resolved `localhost` over IPv6 slowly while ports were bound to IPv4. D-053 records the explicit-loopback fix.
- Acceptance criteria:
  - Infrastructure has fixed, documented local ports: Passed
  - Kafka and PostgreSQL data use named persistent volumes: Passed
  - LocalStack state uses a named persistent volume: Passed
  - Redpanda Console connects to Apache Kafka: Passed
  - Adminer reaches the Compose PostgreSQL service: Passed
  - Unsecured ports are bound only to loopback: Passed

### Step 19B — Local application runtime and visual dashboard

- Date: 2026-08-12
- Application image: `mqtest-local-app:latest`, built once from `Dockerfile.local`
- Runtime services: initializer, dashboard/API, Kafka worker, and SQS worker
- Stable resources: `orders.created.local`, `orders.created.local.dlq`, `orders-created-local`, `orders-created-local-dlq`, `kafka_lab`, and `sqs_lab`
- Focused test command: `.\.venv\Scripts\python.exe -m pytest tests\unit\test_local_lab.py -q`
- Focused result: Passed — 5 local-lab guards
- Browser Kafka result: API and broker acknowledgement completed; Kafka worker stored the row; Console retained the exact key, event ID, correlation ID, payload, and one message
- Browser SQS result: paused consumer left one ready message and no new database row; resume returned queue depth to zero and created the exact row
- Kafka group evidence: `local-order-consumer` committed partition 2 at offset 1 with lag zero
- PostgreSQL UI evidence: Adminer displayed schemas `kafka_lab`, `sqs_lab`, and `local_lab`; `sqs_lab.orders` displayed both browser-created rows
- Runtime log evidence: Kafka reports partition/offset; SQS reports event ID after processing and deletion
- Lifecycle result: `Status`, `Stop`, and `Start -NoBrowser` passed; database counts remained `kafka_lab=1` and `sqs_lab=2` after restart
- Final regression command: `.\.venv\Scripts\python.exe -m pytest -q`
- Final regression result: Passed — 81 tests in 98.46 seconds while all persistent local services were active
- Cleanup result: Disposable Testcontainers services were removed; only intentional `mqtest-local-*` services remained
- Acceptance criteria:
  - One command starts the complete visual system: Passed
  - Dashboard sends through real Kafka and SQS SDK paths: Passed
  - Both continuously running consumers write PostgreSQL: Passed
  - Kafka record remains visible after processing: Passed
  - SQS message waits while paused and is deleted after success: Passed
  - Consumer controls and browser flow report no console errors: Passed
  - Stop preserves data and reset requires explicit confirmation: Passed by script review; destructive reset was not executed

### Step 20 — Clarify application, runtime messaging, and test-helper names

- Date: 2026-08-15
- Decision: D-056
- Application package: `src/order_app`
- Runtime messaging package: `src/order_app/messaging`
- Test-only support package: `tests/helpers`
- Actual test cases: `tests/unit`, `tests/contracts`, and `tests/integration`
- Installed distribution: editable `order-app-messaging-lab==0.1.0`
- Fast verification command: `.\.venv\Scripts\python.exe -m pytest -m "unit or contract" -q`
- Fast result: Passed — 53 selected tests passed, 28 deselected in 6.77 seconds
- Compose verification command: `docker compose -f compose.local.yml config --quiet`
- Compose result: Passed
- Full verification command: `.\.venv\Scripts\python.exe -m pytest -q`
- Full result: Passed — 81 tests in 66.01 seconds
- Compatibility decision: Existing Compose project, container, volume, and PostgreSQL identifiers retain `mqtest` temporarily so the learner's persistent Kafka messages, offsets, queues, and database rows remain attached. They are infrastructure state identifiers, not Python package names.
- Acceptance criteria:
  - No `sample_app` or `order_app_tests` references remain in active code/configuration: Passed
  - Application source does not import from `tests`: Passed
  - Runtime contracts and clients are packaged with `order_app`: Passed
  - Test-only polling, HTTP stub, and Testcontainers support live under `tests/helpers`: Passed
  - Complete regression suite passes: Passed

### Step 21A — Separate application runtime from automated-test support

- Date: 2026-08-15
- Decision: D-057
- Runtime boundary: `src/order_app` contains code called by the API, workers, and local lab
- Test-support boundary: `tests/helpers` contains probes, generated topic/queue names, and Testcontainers image pins
- Refactoring: Split SQS publishing from SQS receive-and-assert behavior; consolidated three image-setting modules into `tests/helpers/container_images.py`
- Fast verification command: `.\.venv\Scripts\python.exe -m pytest -m "unit or contract" -q`
- Fast result: Passed — 53 selected tests passed, 28 deselected in 5.37 seconds
- Full verification command: `.\.venv\Scripts\python.exe -m pytest -q`
- Full result: Passed — 81 tests passed, 1 dependency deprecation warning, in 60.18 seconds
- Acceptance criteria:
  - No application module exposes test observation probes: Passed
  - Generated test resource names live under `tests/helpers`: Passed
  - Container images are defined once under test support: Passed
  - Unit and contract tests remain green: Passed
  - Complete Kafka/SQS regression remains green: Passed

### Step 21B — Make runtime wiring and file ownership explicit

- Date: 2026-08-15
- Decision: D-058
- Business folders: `order_api`, `messaging`, and `order_processing`
- Visual runtime files: `dashboard_api.py`, `consumer_worker.py`, `initialize_resources.py`, `resource_setup.py`, `consumer_control_store.py`, and `config.py`
- Test helpers: Flattened to explicitly named modules directly under `tests/helpers`
- Ownership reference: `docs/code-map.md`
- Stale-name scan: No former runtime import paths or class names remain outside historical decision records
- Fast verification command: `.\.venv\Scripts\python.exe -m pytest -m "unit or contract" -q`
- Fast result: Passed — 53 selected tests passed, 28 deselected in 4.18 seconds
- Collection result: All 81 tests collected successfully
- Compose configuration: Passed; all renamed executable modules import successfully
- Full verification command: `.\.venv\Scripts\python.exe -m pytest -q`
- Full result: Passed — 81 tests passed, 1 dependency deprecation warning, in 77.21 seconds
- Persistent-lab verification: Rebuilt with `scripts/local-lab.ps1 -Action Start -NoBrowser`; dashboard, Kafka worker, and SQS worker use the renamed modules and are healthy
- Preserved-state evidence: Dashboard reported Kafka, SQS, and PostgreSQL up with 5 Kafka orders and 3 SQS orders still present
- Acceptance criteria:
  - File purpose is understandable from its folder plus filename: Passed
  - Runtime and test-helper ownership is documented: Passed
  - No one-file `http` or `infrastructure` helper subpackages remain: Passed
  - Compose runs the renamed modules: Passed
  - Existing persistent learning data survives the rebuild: Passed
  - Complete regression suite remains green: Passed

### Step 21C — Consolidate helpers and broker publishers

- Date: 2026-08-15
- Decision: D-059
- Test-helper result: Exactly four implementation files remain under `tests/helpers`: `kafka.py`, `sqs.py`, `client_stub.py`, and `docker.py`
- Function conversion: Removed `SqsQueueProbe` and `SqsTestResources`; their operations are explicit functions in `tests/helpers/sqs.py`
- Publisher result: `EventPublisher` is the common base; `KafkaEventPublisher` and `SqsEventPublisher` are child implementations in `messaging/event_publishers.py`
- Topic administration: `messaging/kafka_topic_admin.py` remains separate because topic lifecycle is not event publishing
- Fast command: `.\.venv\Scripts\python.exe -m pytest -m "unit or contract" -q`
- Fast result: Passed — 54 selected tests passed, 28 deselected in 4.55 seconds
- Full command: `.\.venv\Scripts\python.exe -m pytest -q`
- Full result: Passed — 82 tests passed, 1 dependency deprecation warning, in 65.57 seconds
- Persistent-lab result: Rebuilt normally with named volumes preserved; dashboard, Kafka, SQS, and PostgreSQL reported healthy
- Runtime hierarchy evidence: Both publisher child classes imported inside the rebuilt dashboard container and passed `issubclass(..., EventPublisher)`
- Preserved-state evidence: 5 Kafka orders and 3 SQS orders remained after the rebuild
- Acceptance criteria:
  - Helper implementation is limited to the four requested domain files: Passed
  - Kafka/SQS publishers use one base/child module: Passed
  - Stateless wrapper classes were converted to functions: Passed
  - Stateful lifecycle classes remain where justified: Passed
  - Full disposable-infrastructure behavior remains green: Passed
  - Persistent visual application remains healthy with data preserved: Passed

## Open decisions

These decisions are intentionally deferred until their implementation step:

| Decision | Target step | Why deferred |
|---|---|---|

## Update protocol

For every implementation step:

1. Change the current position to the new active step.
2. Record decisions before or while implementing them.
3. Record the exact verification command and outcome.
4. Mark the milestone complete only when its acceptance criteria pass.
5. Identify the next gate without silently starting it.
