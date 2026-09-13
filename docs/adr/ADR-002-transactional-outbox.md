# ADR-002: Publish requested events through a transactional outbox

Status: accepted

## Decision

Write the transaction and its requested event to PostgreSQL in one transaction. A separate scheduled publisher delivers the outbox row to Kafka.

## Rationale

Saving to PostgreSQL and publishing to Kafka are a dual write. Direct publish after commit can permanently lose work when the process or broker fails in the gap.

## Consequences

Creation is eventually consistent and outbox operation must be monitored. Duplicate publication remains possible, so consumers must be idempotent. Published-event retention and operational metrics are defined in [ADR-007](ADR-007-outbox-retention-and-monitoring.md).

