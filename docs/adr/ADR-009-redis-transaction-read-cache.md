# ADR-009: Use Redis as a non-authoritative transaction read cache

## Status

Accepted

## Context

Clients poll `GET /transactions/{id}` while asynchronous processing advances a transaction. Those repeated reads otherwise load the same transaction and ordered history from PostgreSQL. The cache must not participate in transaction creation, idempotency, state transitions, or event publication.

Invalidating Redis before a PostgreSQL commit creates a race: a concurrent reader can observe the old database state and repopulate it immediately before the update commits. Requiring Redis for reads would also turn a cache outage into an API outage.

## Decision

Use Redis with cache-aside reads under versioned keys (`ledgerflow:transaction:v1:{id}`) and a configurable 30-second default TTL. A cache miss or Redis failure reads the authoritative transaction and history from PostgreSQL. Successful database reads populate Redis.

Every consumed status event schedules eviction after the surrounding database transaction commits, including duplicate and stale events. Failed transactions do not evict because their synchronization never reaches `afterCommit`. Short Redis connect and command timeouts bound fallback latency.

Redis is excluded from readiness health. Hit, miss, write, eviction, and failure counters expose cache behavior through Actuator and Prometheus.

## Consequences

Repeated status reads avoid PostgreSQL work while cached. Redis data remains disposable and can be flushed or lost without affecting correctness. A failed eviction, or a read that began before a concurrent status commit and repopulates Redis just after eviction, may expose stale state only until the TTL expires. TTL selection is therefore a bounded-staleness and database-load tradeoff. Cache failures increase read latency and database load but do not make the endpoint unavailable.
