# Decisions and Implementation Progress

This is the living implementation record for the project. Update it whenever a step begins, a material decision is made, verification is completed, or the next implementation gate changes.

## Current position

| Item | Current value |
|---|---|
| Active phase | Phase 1 — Kafka foundation |
| Current point | Step 4 — Define the event contract completed |
| Step status | Completed and verified |
| Last completed step | Step 4 — Define the event contract |
| Next gate | Step 5 — Build the Kafka producer client |

## Repository state

| Item | Current value |
|---|---|
| Primary branch | `main` |
| Remote | `origin` → `https://github.com/PrashantSinghT99/kafka-sqs.git` |
| Last completed-step reference | Step 4 — `feat: add versioned order event contract` |
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

## Open decisions

These decisions are intentionally deferred until their implementation step:

| Decision | Target step | Why deferred |
|---|---|---|
| Sample HTTP framework | Step 7 | Select only when the producer API is implemented |
| PostgreSQL client and migration method | Step 9 | Select alongside transactional idempotency design |
| HTTP stub product | Step 11 | Select based on Python Testcontainers support and verification API |

## Update protocol

For every implementation step:

1. Change the current position to the new active step.
2. Record decisions before or while implementing them.
3. Record the exact verification command and outcome.
4. Mark the milestone complete only when its acceptance criteria pass.
5. Identify the next gate without silently starting it.
