# Architecture

## Runtime flow

```mermaid
flowchart TD
    C[Client] --> API[Transaction API]
    API --> PG[(PostgreSQL)]
    PG --> OP[Outbox Publisher]
    OP --> RQ[requested.v1]
    RQ --> PR[Transaction Processor]
    PR --> PS[Provider Simulator]
    PR --> ST[status.v1]
    ST --> API
```

## Creation sequence

```mermaid
sequenceDiagram
    participant C as Client
    participant A as Transaction API
    participant D as PostgreSQL
    participant K as Kafka
    C->>A: POST + Idempotency-Key
    A->>D: insert transaction + history + outbox
    D-->>A: commit
    A-->>C: 202 + transaction ID
    A->>K: publish requested event
    K-->>A: broker acknowledgement
    A->>D: mark outbox published
```

The last two steps are intentionally not atomic. A crash between them produces a duplicate event, not lost work.

Kafka delivery is at least once. When a requested event is redelivered, the processor repeats the provider call with the same transaction ID as its idempotency key. The provider returns the original decision, and the transaction API treats repeated or stale status changes as no-ops. See [ADR-004](adr/ADR-004-at-least-once-consumer-idempotency.md).

Transient processing failures move through non-blocking Kafka retry topics with exponential backoff. Attempts are bounded, after which the record moves to a dead-letter topic for explicit failure handling. See [ADR-005](adr/ADR-005-retry-and-dead-letter-strategy.md).

## Provider sequence

```mermaid
sequenceDiagram
    participant K as Kafka
    participant P as Processor
    participant S as Provider
    participant A as Transaction API
    K->>P: requested event
    P->>K: PROCESSING status
    P->>S: process(transactionId)
    S-->>P: approved or declined
    P->>K: terminal status
    K->>A: status event
    A->>A: enforce transition + deduplicate event
```
