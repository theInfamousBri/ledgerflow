# ADR-004: Idempotency under at-least-once delivery

Status: accepted

## Decision

Accept that Kafka may deliver a requested event more than once. Every provider request uses the LedgerFlow transaction ID as its idempotency key, and the transaction API applies duplicate or stale status events as no-ops.

## Rationale

Kafka acknowledgement cannot be atomically committed with an external HTTP side effect. A processor can therefore finish the provider call and crash before acknowledging the record, causing the same request to be delivered again.

The provider is the system that owns the external side effect, so its idempotency contract is the final protection against creating the payment twice. The transaction API independently protects authoritative state and history from repeated status events.

## Consequences

- A redelivered Kafka record may produce another HTTP request.
- Repeated requests return the provider's original decision and reference.
- Repeated or stale status events do not add history or move a terminal transaction backward.
- The provider simulator exposes payment lookup and request-count diagnostics so integration tests can prove that a redelivery occurred without creating another logical effect.
- A durable processor inbox/outbox remains a future option if the real provider cannot guarantee idempotency or repeated calls are prohibitively expensive.
