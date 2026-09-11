# LedgerFlow Technical Specification

Status: initial implementation baseline  
Target: Java 21, Spring Boot 3.5.x, Kafka, PostgreSQL

## 1. Purpose and scope

LedgerFlow accepts payment/refund requests, creates an authoritative transaction record, processes work asynchronously through a simulated provider, and exposes current status plus immutable history.

The system demonstrates correctness under duplicate delivery, process restarts, delayed messaging, downstream failure, and the database/Kafka dual-write boundary. It is not a card network, ledger/accounting engine, or Stripe clone.

## 2. Service ownership

| Component | Owns | Does not own |
| --- | --- | --- |
| transaction-api | transaction state, history, API idempotency, requested-event outbox | provider execution |
| transaction-processor | provider orchestration and retry policy | authoritative transaction state |
| payment-provider-simulator | simulated provider decisions keyed by transaction ID | platform transaction lifecycle |
| PostgreSQL | authoritative transaction and outbox data | ephemeral cache data |
| Kafka | asynchronous delivery | source-of-record state |

The transaction API consumes status-change events and alone applies changes to authoritative state. The processor never writes transaction tables directly.

## 3. REST API

### POST /transactions

Required header: `Idempotency-Key` (1–100 printable characters)

```json
{
  "accountId": "acct-42",
  "amount": 125.50,
  "currency": "USD",
  "type": "PAYMENT"
}
```

Rules:

- `amount` must be positive and have at most two fractional digits in MVP.
- `currency` must have the uppercase three-letter ISO-4217 shape; an allowlist is a hardening milestone.
- The same key and semantically identical payload returns the original transaction.
- The same key with a different payload returns `409 Conflict`.
- A newly accepted request returns `202 Accepted` with `Location: /transactions/{id}`.
- A replay returns `200 OK` and the same resource.

### GET /transactions/{id}

Returns current state and ordered status history. Unknown IDs return `404`.

```json
{
  "id": "6a136b64-b2bd-4ec2-849a-ad00cb1ef272",
  "accountId": "acct-42",
  "amount": 125.50,
  "currency": "USD",
  "type": "PAYMENT",
  "status": "PROCESSING",
  "createdAt": "2026-09-11T01:00:00Z",
  "updatedAt": "2026-09-11T01:00:01Z",
  "history": [
    {"status":"PENDING","occurredAt":"2026-09-11T01:00:00Z"},
    {"status":"PROCESSING","occurredAt":"2026-09-11T01:00:01Z"}
  ]
}
```

Errors use RFC 9457 Problem Details (`application/problem+json`).

## 4. State machine

Allowed transitions:

| From | To | Meaning |
| --- | --- | --- |
| none | PENDING | request durably accepted |
| PENDING | PROCESSING | processor began provider work |
| PROCESSING | COMPLETED | provider approved |
| PROCESSING | FAILED | provider produced a terminal rejection or retry policy was exhausted |

Terminal states never transition. Duplicate events that repeat the current state and late events from an earlier lifecycle stage are no-ops. Conflicting terminal outcomes and illegal forward transitions are rejected and metered. Optimistic locking prevents concurrent lost updates.

## 5. Kafka contracts

Topics:

| Topic | Key | Value |
| --- | --- | --- |
| `ledgerflow.transaction.requested.v1` | transaction ID | `TransactionRequestedEvent` JSON |
| `ledgerflow.transaction.status.v1` | transaction ID | `TransactionStatusChangedEvent` JSON |
| `ledgerflow.transaction.requested.v1-dlt` | transaction ID | original failed record |

Partitioning by transaction ID preserves order for one transaction. Events contain `eventId`, `schemaVersion`, `transactionId`, `traceId`, and `occurredAt`. Additive changes retain the topic version; breaking changes require a new topic/consumer migration.

Kafka delivery is at least once. Consumers must assume duplicates. The provider call uses `transactionId` as the provider idempotency key. A production iteration will add a durable processor inbox/outbox so correctness does not depend exclusively on provider behavior.

## 6. Persistence

### transactions

- UUID primary key
- globally unique `idempotency_key`
- SHA-256 `request_fingerprint` used to reject key/payload mismatches
- amount `numeric(19,2)` plus three-character currency
- state, provider reference, failure code, timestamps, optimistic-lock version

Creation also acquires a transaction-scoped PostgreSQL advisory lock derived from the idempotency key. This serializes concurrent submissions of the same key before the lookup/insert while the unique constraint remains the final invariant.

### transaction_status_history

Append-only history with transaction ID, state, reason, event ID, and occurrence time. A unique event-ID constraint makes result-event application idempotent.

### outbox_events

Created atomically with the transaction. A scheduled publisher sends unpublished rows and marks them published only after broker acknowledgement. A crash after broker acknowledgement but before the mark can republish, so downstream consumers remain idempotent.

## 7. Correctness invariants

1. One idempotency key identifies at most one transaction.
2. A committed transaction always has a committed requested outbox event.
3. Kafka unavailability never rolls back an already accepted transaction; publishing catches up later.
4. Only the transaction API mutates authoritative transaction state.
5. Every accepted state transition creates exactly one history row per event ID.
6. Terminal transaction states are immutable.
7. Provider retries use the same provider idempotency identifier.

## 8. Failure behavior

| Failure | Required behavior |
| --- | --- |
| API process fails before commit | no transaction exists; client safely retries |
| API process fails after commit | client retry returns existing transaction |
| Kafka unavailable | outbox remains pending; publisher retries |
| requested event redelivered | provider idempotency prevents duplicate effect |
| provider timeout/5xx | exponential retry; then DLT |
| result event redelivered | event ID uniqueness makes application a no-op |
| out-of-order state event | state machine rejects it and emits a metric/log |
| transaction stuck in PROCESSING | future reconciler queries provider state and repairs it |

## 9. Security and data handling

- No PAN, CVV, credentials, or real customer data.
- Validate request size and fields at the edge.
- Do not log idempotency keys or full account identifiers at INFO level.
- Secrets arrive through environment/secret stores, never source control.
- Authentication/authorization are explicitly deferred from MVP and required before public deployment.

## 10. Observability

Every request/event carries W3C trace context when OpenTelemetry is added. Initial logs include transaction ID, event ID, and trace ID as structured fields. Actuator exposes liveness/readiness and Micrometer metrics.

Required future dashboards: API latency/error rate; outbox age/backlog; Kafka consumer lag; provider latency/failure/circuit state; transitions by result; reconciliation repairs.

## 11. Acceptance tests by milestone

The MVP end-to-end Testcontainers test proves POST → outbox → Kafka → processor → provider → result event → COMPLETED, verifies ordered history and the published outbox row, and confirms that an identical idempotent replay returns the original resource.

The end-to-end suite covers concurrent duplicate submissions. Further hardening adds tests for key/payload conflict, requested-event redelivery, provider timeouts, retries, DLT routing, Kafka outage recovery, outbox duplicate publication, illegal transitions, and stuck-state reconciliation.

## 12. Explicit deferrals

Redis, circuit breaking, reconciliation, OpenTelemetry collector/dashboard stack, Kubernetes, AWS, CI/CD, and load tests are planned but are not represented as complete in this first repository skeleton.
