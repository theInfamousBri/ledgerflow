# Operating LedgerFlow

## Outbox signals

| Metric | Type | Interpretation |
| --- | --- | --- |
| `ledgerflow.outbox.pending` | Gauge | Current events waiting for Kafka acknowledgement |
| `ledgerflow.outbox.oldest.pending.age` | Gauge | Seconds since the oldest unpublished event was created |
| `ledgerflow.outbox.publish.success` | Counter | Events acknowledged by Kafka and marked published |
| `ledgerflow.outbox.publish.failure` | Counter | Failed Kafka publication attempts |
| `ledgerflow.outbox.cleanup.deleted` | Counter | Published rows removed after retention |
| `ledgerflow.outbox.metrics.refresh.failure` | Counter | Failed PostgreSQL reads while refreshing gauge snapshots |
| `ledgerflow.reconciliation.scan` | Counter | Completed scans for stale processing transactions |
| `ledgerflow.reconciliation.requested` | Counter | Stale transactions durably queued for reconciliation |
| `ledgerflow.reconciliation.resolved` | Counter | Provider decisions converted into terminal status events |
| `ledgerflow.reconciliation.unresolved` | Counter | Lookups with no provider record or no completed decision |
| `ledgerflow.reconciliation.failure` | Counter | Reconciliation messages that exhausted processing retries |
| `ledgerflow.cache.transaction.hit` | Counter | Transaction reads served from Redis |
| `ledgerflow.cache.transaction.miss` | Counter | Redis misses followed by an authoritative database lookup |
| `ledgerflow.cache.transaction.write` | Counter | Database responses successfully cached with a TTL |
| `ledgerflow.cache.transaction.eviction` | Counter | Cache eviction commands issued after status commits |
| `ledgerflow.cache.transaction.failure` | Counter | Redis access or cached-payload failures handled by fallback |

Backlog and age should be evaluated together. A brief backlog increase with continuing publication successes is normal under load. A growing oldest-event age, especially alongside publication failures, indicates that accepted transactions are not reaching Kafka.

The gauges are refreshed independently of Prometheus scraping. Their maximum expected staleness is `ledgerflow.outbox.metrics-refresh-delay` unless the refresh-failure counter is increasing.

## Initial response to a growing outbox

1. Check transaction API health and PostgreSQL connectivity.
2. Check Kafka broker availability and authentication/network errors in transaction API logs.
3. Compare the publication success and failure counter rates.
4. Inspect the oldest unpublished rows without modifying them:

   ```sql
   SELECT id, aggregate_id, topic, created_at
   FROM outbox_events
   WHERE published_at IS NULL
   ORDER BY created_at
   LIMIT 20;
   ```

5. Restore the failed dependency and allow the publisher to drain the backlog.

Never delete unpublished rows to clear an alert. They represent committed work that has not yet received Kafka acknowledgement.

## Retention configuration

| Environment variable | Default | Purpose |
| --- | --- | --- |
| `OUTBOX_RETENTION` | `7d` | Time to retain successfully published rows |
| `OUTBOX_CLEANUP_BATCH_SIZE` | `1000` | Maximum rows deleted in one transaction |
| `OUTBOX_CLEANUP_DELAY` | `1h` | Delay between completed cleanup runs |
| `OUTBOX_CLEANUP_INITIAL_DELAY` | `1m` | Startup delay before the first cleanup run |
| `OUTBOX_METRICS_REFRESH_DELAY` | `15s` | Delay between metric snapshots |
| `OUTBOX_METRICS_INITIAL_DELAY` | `5s` | Startup delay before the first metric snapshot |

Cleanup is deliberately absent from readiness and liveness health. A temporary retention failure should alert operators, not cause an orchestrator to restart an otherwise healthy API instance.

## Transaction read cache

Redis accelerates repeated `GET /transactions/{id}` requests and contains no authoritative data. The API uses 200-millisecond connection and command timeouts by default, falls back to PostgreSQL on any Redis access failure, and does not include Redis in readiness health.

A falling hit ratio may indicate a TTL that is too short for the polling pattern. A rising failure counter should be investigated even while requests succeed, because database load and read latency will increase. Failed evictions and the residual concurrent-read race inherent in cache-aside invalidation are bounded by the TTL; do not extend the TTL without considering the longer stale-read window.

| Environment variable | Default | Purpose |
| --- | --- | --- |
| `REDIS_HOST` | `localhost` | Redis host used by the transaction API |
| `REDIS_PORT` | `6379` | Redis port |
| `REDIS_CONNECT_TIMEOUT` | `200ms` | Maximum connection establishment time before fallback |
| `REDIS_COMMAND_TIMEOUT` | `200ms` | Maximum Redis command time before fallback |
| `TRANSACTION_CACHE_TTL` | `30s` | Maximum lifetime of a cached transaction response |

## Reconciliation

Reconciliation candidates are transactions whose status remains `PROCESSING` beyond `RECONCILIATION_STALE_AFTER`. Each claimed row records when reconciliation was requested and increments its attempt count. Unresolved rows cannot be requested again until `RECONCILIATION_RETRY_DELAY` passes.

Investigate sustained unresolved growth by comparing provider request history with the transaction's status history. Do not manually force a terminal state without an authoritative provider decision.

| Environment variable | Default | Purpose |
| --- | --- | --- |
| `RECONCILIATION_STALE_AFTER` | `2m` | Minimum `PROCESSING` age before a transaction is eligible |
| `RECONCILIATION_RETRY_DELAY` | `5m` | Minimum delay before re-requesting an unresolved transaction |
| `RECONCILIATION_BATCH_SIZE` | `100` | Maximum candidates claimed by one scan |
| `RECONCILIATION_SCAN_DELAY` | `30s` | Delay between completed scans |
| `RECONCILIATION_INITIAL_DELAY` | `30s` | Startup delay before the first scan |
