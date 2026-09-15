# ADR-001: PostgreSQL is the source of record

Status: accepted

## Decision

Store authoritative current transaction state, immutable status history, idempotency ownership, and the transactional outbox in PostgreSQL.

## Rationale

The system requires uniqueness, atomic multi-row writes, constraints, and durable queryable history. PostgreSQL provides these without making cache or broker availability part of correctness.

## Consequences

All state mutations route through the transaction API. Redis accelerates retrieval reads but cannot decide whether a duplicate transaction exists or whether a state transition is valid.

