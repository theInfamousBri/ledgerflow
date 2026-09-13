# ADR-007: Bound outbox retention and expose operational metrics

Status: accepted

## Decision

Retain successfully published outbox events for seven days by default, then delete them in configurable batches. Select cleanup rows with PostgreSQL `FOR UPDATE SKIP LOCKED` so multiple transaction API instances can perform maintenance without selecting the same batch.

Expose scheduled Micrometer snapshots for unpublished backlog count and oldest-event age. Count successful publications, publication failures, cleanup deletions, and metric-refresh failures.

## Rationale

Published rows are useful for short-term diagnosis but cannot grow without bound. Unpublished rows represent work that must not be lost, so they are never eligible for retention cleanup. Backlog size alone is insufficient: a small but old backlog can reveal a stuck publisher, while age alone does not show incident scale.

Refreshing database-backed gauges on a schedule decouples PostgreSQL query load from Prometheus scrape frequency and dashboard fan-out.

## Consequences

- Published event payloads remain available only for the configured retention window.
- Cleanup deletes at most one configured batch per run, limiting transaction duration and lock pressure.
- Multiple application instances may run cleanup concurrently without requiring a distributed scheduler lock.
- Gauge values are snapshots and can be stale by up to the configured refresh delay.
- Metric-refresh failures preserve the last successful snapshot and increment a dedicated failure counter.
- Cleanup/backlog conditions do not mark application health `DOWN`; alerts should not trigger restart loops during a Kafka or maintenance incident.
