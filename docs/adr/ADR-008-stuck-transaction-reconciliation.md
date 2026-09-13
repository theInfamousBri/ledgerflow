# ADR-008: Reconcile stale processing transactions asynchronously

Status: accepted

## Decision

The transaction API scans `PROCESSING` transactions older than a configurable threshold. It locks a bounded candidate batch with PostgreSQL `FOR UPDATE SKIP LOCKED`, records the request time and attempt count, and writes a reconciliation request to the transactional outbox in the same database transaction.

The transaction processor consumes reconciliation requests because it owns provider integration. It performs a read-only lookup using the transaction ID. A recorded provider decision produces the normal terminal status event; a missing record or incomplete decision leaves the transaction unchanged for a later scan.

## Rationale

An asynchronous provider call can succeed while its terminal status event is lost or delayed. Transactions must not remain in `PROCESSING` forever, but absence of a provider decision is not evidence of failure.

Writing reconciliation requests through the existing outbox preserves them during Kafka outages. Keeping provider lookup in the processor avoids introducing a second provider client and resilience policy into the transaction API.

## Consequences

- Reconciliation is eventually consistent and bounded by the stale threshold, scan interval, Kafka delivery, and status-event processing time.
- Multiple API instances can scan concurrently without claiming the same row in one pass.
- The request timestamp throttles unresolved transactions until the configured retry delay expires.
- Duplicate requests and terminal status events are safe because Kafka keys preserve transaction ordering and the existing state application treats repeated terminal states as no-ops.
- Provider lookup failures use bounded retry topics and a DLT; the transaction remains eligible for a later scan rather than being falsely failed.
- Reconciliation never calls the provider's mutation endpoint and therefore cannot create another payment effect.
