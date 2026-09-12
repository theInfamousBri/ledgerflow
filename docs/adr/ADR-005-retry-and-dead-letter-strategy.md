# ADR-005: Retry and dead-letter strategy

Status: accepted

## Decision

Use Kafka retry topics for provider-processing failures. A requested event receives at most four total processing attempts with exponential delays starting at 500 milliseconds, doubling on each retry, and capped at five seconds. An event that exhausts the policy is routed to the requested-event dead-letter topic.

## Rationale

Transient provider failures should not immediately fail a transaction, but blocking a Kafka consumer thread during backoff reduces throughput and can interfere with consumer-group stability. Retry topics defer the next attempt without holding the original listener thread.

The bounded attempt count prevents poison messages and persistent downstream failures from retrying forever. Exponential delays reduce pressure on a dependency while it recovers.

## Consequences

- A successful retry completes the original transaction without duplicate history entries.
- Every attempt repeats the provider request with the transaction ID as its idempotency key.
- Recovery is eventually consistent and takes at least the accumulated backoff time.
- The DLT handler marks the transaction `FAILED` with `RETRIES_EXHAUSTED`; operational visibility and replay tooling remain future work.
- The provider simulator supports deterministic fail-first plans and attempt timestamps so retry behavior can be verified without random tests.
