# Step-by-Step Implementation Plan

This plan intentionally introduces one messaging concept at a time. Every step must leave the repository runnable and include a small learning checkpoint before moving forward.

Live status and material choices are recorded in [Decisions and Implementation Progress](decisions.md).

## Delivery rules

- Implement one step at a time and review its output before beginning the next.
- Keep the first scenario small: `POST /orders` publishes `order.created`.
- Pin dependency and container-image versions; do not use `latest` tags.
- Each integration test owns its topic, queue, consumer group, IDs, and cleanup.
- Use bounded polling instead of `sleep()`.
- Store no cloud credentials in the repository.
- Run unit tests before integration tests.

## Phase 1: Kafka foundation

### Step 1 — Bootstrap the Python project

Status: Completed on 2026-08-12. Verification is recorded in [Decisions and Implementation Progress](decisions.md).

Goal: create the smallest maintainable test project.

Tasks:

1. Add `pyproject.toml` with Python 3.12 metadata.
2. Add pytest and development-tool configuration.
3. Create importable `mqtest` and `sample_app` packages.
4. Add one unit smoke test.
5. Add `.gitignore` for Python, test, environment, and report outputs.
6. Add commands to the README for environment setup and unit tests.

Acceptance criteria:

- A fresh virtual environment can install the project.
- `pytest -m unit` passes without Docker.
- No application or broker behavior exists yet.

Learning checkpoint: explain the difference between the reusable test harness and the sample system under test.

### Step 2 — Start disposable Kafka

Status: Completed on 2026-08-12. Verification is recorded in [Decisions and Implementation Progress](decisions.md).

Goal: prove the tests can manage a real Kafka broker lifecycle.

Tasks:

1. Add Testcontainers Kafka dependency.
2. Add a session-scoped Kafka container fixture.
3. Pin an Apache Kafka container image.
4. Add readiness diagnostics and a clear Docker-unavailable error.
5. Add a Kafka connectivity test using AdminClient.

Acceptance criteria:

- Kafka starts automatically when the integration suite begins.
- The test obtains a working bootstrap-server address.
- Kafka stops and cleans up after the suite.
- Failure output is understandable when Docker is not running.

Learning checkpoint: identify broker, bootstrap server, topic, partition, and client.

### Step 3 — Provision isolated topics

Status: Completed on 2026-08-12. Verification is recorded in [Decisions and Implementation Progress](decisions.md).

Goal: prevent test runs from sharing topic state.

Tasks:

1. Build a unique resource-name generator using the test/run ID.
2. Add AdminClient helpers to create, describe, and delete topics.
3. Add a function-scoped topic fixture.
4. Default the learning topic to multiple partitions so routing can later be tested.
5. Record topic configuration in test evidence.

Acceptance criteria:

- Parallel tests receive different topic names.
- A created topic is ready before a producer uses it.
- Topic metadata and partition count can be asserted.

Learning checkpoint: explain why an offset is only meaningful with its topic and partition.

### Step 4 — Define the event contract

Status: Completed on 2026-08-12. Verification is recorded in [Decisions and Implementation Progress](decisions.md).

Goal: create a consistent, testable message envelope.

Tasks:

1. Add the `order.created` JSON Schema.
2. Add typed models for the envelope and order payload.
3. Add an event factory that generates unique IDs and timestamps.
4. Add contract validation helpers.
5. Test valid, missing-field, wrong-type, and unsupported-version examples.

Acceptance criteria:

- Valid events serialize and validate.
- Invalid events fail with field-level diagnostics.
- Tests demonstrate the difference among event, correlation, and causation IDs.

Learning checkpoint: explain why `event_id`, not `correlation_id`, is used for duplicate protection.

## Phase 2: Kafka producer testing

### Step 5 — Build the Kafka producer client

Status: Completed on 2026-08-12. Verification is recorded in [Decisions and Implementation Progress](decisions.md).

Goal: publish controlled events from tests.

Tasks:

1. Wrap the Confluent Producer with explicit configuration.
2. Serialize the event envelope to JSON.
3. Publish with order ID as the Kafka key.
4. Add event type, version, correlation ID, and content type as headers.
5. Wait for and validate the delivery callback.
6. Capture topic, partition, and offset returned by Kafka.

Acceptance criteria:

- A controlled event is durably acknowledged by Kafka.
- Delivery failure raises a test-friendly exception.
- Returned broker metadata appears in test output.

Learning checkpoint: explain how the Kafka key affects partition routing and ordering.

### Step 6 — Build the Kafka test probe

Status: Completed on 2026-08-12. Verification is recorded in [Decisions and Implementation Progress](decisions.md).

Goal: observe producer output independently.

Tasks:

1. Create a Consumer with a unique group ID.
2. Disable auto-commit.
3. Make offset-start behavior explicit.
4. Implement `wait_for_event` using a correlation/event predicate and deadline.
5. Skip unrelated records while retaining compact diagnostic evidence.
6. Close the Consumer reliably.

Acceptance criteria:

- The probe starts before the trigger and finds the intended event.
- Unrelated records do not cause a false assertion.
- Missing events fail at the deadline, not through an endless poll.
- The error includes the group ID and records observed.

Learning checkpoint: explain why a unique Kafka consumer group observes rather than steals an application event.

### Step 7 — Add the sample producer API

Status: Completed on 2026-08-12. Verification is recorded in [Decisions and Implementation Progress](decisions.md).

Goal: test a realistic API-to-Kafka producer boundary.

Tasks:

1. Add a minimal `POST /orders` service.
2. Accept or create an `X-Correlation-ID`.
3. Validate the order request.
4. Construct and publish `order.created`.
5. Return the order ID and correlation ID.
6. Add unit tests for request validation and event mapping.

Acceptance criteria:

- A valid request returns an accepted/success response.
- The API propagates the correlation ID to the event.
- Invalid requests do not publish events.

Learning checkpoint: identify which assertions belong to API unit tests and which belong to broker integration tests.

### Step 8 — Complete the Kafka producer component test

Status: Completed on 2026-08-12. Verification is recorded in [Decisions and Implementation Progress](decisions.md).

Goal: verify API action to broker record without involving the business consumer.

Tasks:

1. Start the Kafka probe.
2. Send a unique API request.
3. Wait for the matching correlation ID.
4. Assert destination, key, headers, schema, and business payload.
5. Add a negative test proving invalid API input produces no matching event.

Acceptance criteria:

- The positive test proves the full API-to-Kafka producer boundary.
- The consumer service is not required or running.
- The negative assertion uses a short bounded observation window.

Learning checkpoint: diagnose separately an API mapping failure, publish failure, and probe configuration failure.

## Phase 3: Kafka consumer testing

### Step 9 — Add disposable PostgreSQL and the sample consumer

Status: Completed on 2026-08-12. Verification is recorded in [Decisions and Implementation Progress](decisions.md).

Goal: create a real observable consumer side effect.

Tasks:

1. Add a Testcontainers PostgreSQL fixture.
2. Create `orders` and `processed_events` tables.
3. Implement an order consumer that validates and handles `order.created`.
4. Update business data and record the event ID in one database transaction.
5. Commit the Kafka offset only after successful processing.

Acceptance criteria:

- A valid SDK-published event creates exactly one order record.
- The processed event ID is recorded.
- The offset is not committed when the database transaction fails.

Learning checkpoint: draw the two crash windows around database commit and Kafka offset commit.

### Step 10 — Build eventual assertions and the consumer component test

Status: Completed on 2026-08-12. Verification is recorded in [Decisions and Implementation Progress](decisions.md).

Goal: verify asynchronous results without fixed sleeps.

Tasks:

1. Implement a generic bounded `eventually` helper.
2. Include attempt count, elapsed time, and last observed value on failure.
3. Publish a controlled event through the Kafka client.
4. Poll PostgreSQL until the expected order exists.
5. Assert all business fields and processed-event state.

Acceptance criteria:

- The test succeeds immediately after the result appears.
- A missing result fails within a configured deadline.
- No test contains `time.sleep()` for synchronization.

Learning checkpoint: explain why a bounded poll is faster and less flaky than a fixed delay.

### Step 11 — Add downstream HTTP verification

Status: Completed on 2026-08-12. Verification is recorded in [Decisions and Implementation Progress](decisions.md).

Goal: demonstrate a second observable consumer effect.

Tasks:

1. Start a disposable HTTP stub.
2. Configure the consumer to call it after order creation.
3. Verify method, path, headers, correlation ID, and body.
4. Add configurable success and failure responses.

Acceptance criteria:

- A successful event makes the expected downstream request.
- Correlation ID is propagated.
- The consumer handles a temporary downstream failure according to policy.

Learning checkpoint: distinguish state verification from interaction verification.

## Phase 4: Kafka reliability

### Step 12 — Prove idempotency

Status: Completed on 2026-08-12. Verification is recorded in [Decisions and Implementation Progress](decisions.md).

Goal: make duplicate delivery safe.

Tasks:

1. Publish the same event twice with the same `event_id`.
2. Verify one order business result.
3. Verify the duplicate does not repeat the downstream effect.
4. Publish a different event with the same correlation ID and prove it is not discarded.

Acceptance criteria:

- Same event ID is handled once.
- Same correlation ID with a different event ID remains valid.
- Idempotency check and business update are atomic.

Learning checkpoint: explain why checking and inserting an event ID in separate transactions is unsafe.

### Step 13 — Add retry and DLQ behavior

Status: Completed on 2026-08-12. Verification is recorded in [Decisions and Implementation Progress](decisions.md).

Goal: verify transient and permanent failure policies.

Tasks:

1. Define retry count, backoff, and non-retryable errors.
2. Test temporary HTTP failure followed by success.
3. Publish a poison event that always fails.
4. Verify retry attempts and final DLQ record.
5. Verify no partial business data remains.

Acceptance criteria:

- Retry count is deterministic and observable.
- Poison event reaches the DLQ with original identifiers and failure metadata.
- The main consumer can continue processing a later valid event.

Learning checkpoint: classify validation, transient dependency, and permanent business failures.

### Step 14 — Add ordering, recovery, and transaction scenarios

Status: Completed on 2026-08-12. Verification is recorded in [Decisions and Implementation Progress](decisions.md).

Goal: exercise Kafka-specific reliability guarantees.

Tasks:

1. Publish multiple events with the same key and assert per-partition order.
2. Publish events with different keys and avoid assuming global order.
3. Restart the consumer and verify recovery from committed offsets.
4. Add a `read_committed` probe scenario for committed and aborted transactions.

Acceptance criteria:

- Per-key ordering is demonstrated without claiming topic-wide ordering.
- Restart does not lose a successfully uncommitted event.
- Aborted transactional records are excluded from `read_committed` results.

Learning checkpoint: explain the relationship among key, partition, offset, and consumer group.

## Phase 5: SQS

### Step 15 — Start LocalStack and provision isolated queues

Status: Completed on 2026-08-12. Verification is recorded in [Decisions and Implementation Progress](decisions.md).

Goal: introduce SQS without reusing Kafka assumptions.

Tasks:

1. Add a pinned LocalStack Testcontainer.
2. Create dedicated standard, FIFO, and DLQ queues.
3. Configure visibility timeout, long polling, and redrive policy.
4. Build unique queue naming and cleanup.
5. Add a boto3 connectivity test.

Acceptance criteria:

- Each test owns its queue.
- Queue attributes match the test scenario.
- No producer test consumes from a queue shared with a real consumer.

Learning checkpoint: explain why an SQS test probe can steal/hide a message while a Kafka test group does not.

### Step 16 — Implement SQS producer and consumer component tests

Goal: run the common business scenarios through SQS-specific clients.

Tasks:

1. Build boto3 publishing and receiving helpers.
2. Map event metadata to message attributes.
3. Use long polling with a bounded overall deadline.
4. Test API-to-isolated-queue producer behavior.
5. Test SDK-to-consumer business effects.
6. Delete messages only after successful owned-queue processing.

Acceptance criteria:

- Producer and consumer tests pass through isolated SQS resources.
- Receipt handles and visibility behavior remain explicit.
- Kafka-specific terms do not appear in the SQS adapter API.

Learning checkpoint: compare Kafka offset commit with SQS message deletion.

### Step 17 — Add SQS reliability scenarios

Goal: validate queue-specific failure behavior.

Tasks:

1. Receive without deleting and verify redelivery after visibility timeout.
2. Verify approximate receive count increases.
3. Verify poison-message redrive to DLQ.
4. Verify FIFO order within one message group.
5. Verify deduplication behavior within its supported window.

Acceptance criteria:

- Redelivery and DLQ behavior are observable and bounded.
- FIFO assertions are scoped to a message group.
- Tests still require business idempotency rather than assuming no duplicates.

Learning checkpoint: distinguish visibility, acknowledgement, deduplication, and idempotency.

## Phase 6: CI and reporting

### Step 18 — Add continuous integration and evidence

Goal: make results reproducible outside a developer laptop.

Tasks:

1. Add separate unit, contract, Kafka integration, and SQS integration jobs.
2. Cache only safe package dependencies, not broker state.
3. Publish JUnit XML and structured failure evidence.
4. Add timeouts for jobs, containers, and eventual assertions.
5. Document Docker and resource requirements.

Acceptance criteria:

- A clean CI runner can execute the full suite.
- Failed tests expose correlation ID, event ID, destination, and broker metadata.
- Unit/contract failures return before container suites start.

Learning checkpoint: use a CI failure report to trace one event from input to final observed state.

## Later extensions

These are deliberately postponed until the baseline is reliable:

- Confluent Schema Registry and compatibility gates.
- Avro or Protobuf serialization.
- SASL/TLS and managed Kafka smoke tests.
- Real AWS SQS smoke tests with short-lived credentials.
- Network fault injection with Toxiproxy.
- Consumer lag assertions and operational metrics.
- Performance, soak, and capacity tests.
- Allure dashboards.
- A C#/.NET adapter demonstrating the same architecture in another SDK ecosystem.

## Immediate next action

Begin Step 16 only: build boto3 publisher/receiver adapters, prove the API-to-owned-queue producer boundary, and prove an SDK event creates the expected consumer database effect with deletion only after success.
