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
