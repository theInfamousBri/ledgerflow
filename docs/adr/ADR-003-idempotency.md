# ADR-003: PostgreSQL-enforced API idempotency

Status: accepted

## Decision

Require an `Idempotency-Key` header, enforce uniqueness in PostgreSQL, and store a canonical request fingerprint.

## Rationale

A cache-only check has race and eviction windows. A database uniqueness constraint remains correct across concurrent requests and restarts. The fingerprint prevents accidental reuse of a key for a different operation.

## Consequences

Duplicate identical requests return the original resource. Duplicate keys with different payloads return `409 Conflict`. Redis caches retrieval responses but is not used to decide idempotency and is not required for correctness.
