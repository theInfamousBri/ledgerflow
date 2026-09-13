# Testing LedgerFlow

## Test layers

| Layer | Maven phase | Docker | Purpose |
| --- | --- | --- | --- |
| Unit | `test` | No | Fast domain and component behavior |
| End-to-end | `integration-test` / `verify` | Yes | Real PostgreSQL, Kafka, HTTP, and all three applications |

The `ledgerflow-e2e-tests` module is last in the reactor and uses Maven Failsafe, so its `*IT` tests run during `verify` rather than Surefire's unit-test phase.

## End-to-end topology

`TransactionFlowIT` starts ephemeral PostgreSQL and Kafka containers on random host ports. It then boots the provider simulator, processor, and transaction API in the test JVM on random web ports. No manually running Compose services or fixed ports are required.

The tests verify:

1. A new request returns `202 Accepted` in `PENDING` state.
2. The outbox publisher delivers the requested event through Kafka.
3. The processor calls the real provider simulator.
4. Status events produce exactly `PENDING → PROCESSING → COMPLETED` history.
5. The provider reference is retained.
6. Reusing the same idempotency key and payload returns `200 OK`, the same transaction ID, and the same provider reference.
7. PostgreSQL contains one transaction and one published outbox record.
8. Ten requests released simultaneously with the same idempotency key produce exactly one `202`, nine `200` responses, and one shared transaction ID.
9. Concurrent duplicate requests still produce only one transaction row and one published outbox event before completing normally.
10. Republishing the original requested event causes a real Kafka redelivery and a second provider request using the same transaction ID as its idempotency key.
11. The provider returns its original decision, while the authoritative transaction retains one provider reference and exactly three history entries.
12. A provider configured to fail its first two requests is called exactly three times before the transaction completes.
13. Recorded attempt times prove the retry delays include the configured 500-millisecond and 1,000-millisecond exponential backoffs.
14. Repeated `PROCESSING` events emitted by retries do not create duplicate history entries.
15. Four consecutive provider failures exhaust the configured processing attempts and place the original requested event on the dead-letter topic.
16. The DLT handler transitions the authoritative transaction to `FAILED` with failure code `RETRIES_EXHAUSTED`.
17. Permanent failure retains exactly `PENDING → PROCESSING → FAILED` history and no provider reference.
18. A provider operation that exceeds the client read timeout causes two timed-out attempts before a later retry retrieves the stored provider decision.
19. Timeout recovery uses one provider reference, reaches `COMPLETED`, and retains exactly `PENDING → PROCESSING → COMPLETED` history.
20. Four counted provider failures open the payment-provider circuit breaker.
21. A call made while the circuit is open fails fast without reaching the provider.
22. A successful half-open probe closes the circuit and preserves the provider's idempotent response.
23. Retention cleanup deletes an expired published outbox event while preserving its authoritative transaction.
24. Cleanup increments its Micrometer deletion counter by the number of rows removed.
25. A completed provider decision can repair a transaction whose terminal status was lost and whose authoritative state remains `PROCESSING`.
26. Reconciliation uses the provider's read-only lookup, preserves the original provider reference, and does not create another provider attempt.
27. Reconciliation writes one published outbox request, increments its request/attempt metrics, and retains exactly `PENDING → PROCESSING → COMPLETED` history.

The redelivery scenario intentionally does not claim exactly-once execution. The processor can call an external dependency again after Kafka redelivery. LedgerFlow instead requires an idempotent provider contract and idempotent state application so the repeated attempt cannot create a second payment effect or corrupt transaction history.

The provider's fail-first control is deterministic and one-shot. This keeps retry tests repeatable while leaving the random failure-rate option available for exploratory local testing.

The dead-letter assertion uses a separate Kafka consumer group with `earliest` offset behavior. This verifies the retained DLT key and payload independently of the application's DLT handler.

The E2E processor uses a 250-millisecond provider read timeout, while the delayed simulator operation takes 1.5 seconds. This produces real client timeouts quickly; production defaults remain one second for connect and two seconds for read.

Circuit-breaker state is reset before each scenario so retry, DLT, timeout, and circuit-transition assertions cannot influence one another. The circuit scenario uses a four-call test window for speed; the processor's production configuration uses a 20-call window, a minimum of 10 calls, and a 50% failure-rate threshold.

The cleanup scenario first confirms the event was published, moves only that row beyond the seven-day retention window, and invokes one maintenance batch. Recent published events and all unpublished events remain ineligible. Unit coverage verifies the bounded cutoff calculation plus scheduled backlog and oldest-age metric snapshots.

The reconciliation scenario first completes a real provider operation, then constructs the precise database state produced if its terminal status were lost. A bounded scan writes a reconciliation request through the outbox, the processor retrieves the existing decision without another provider mutation, and the normal status consumer repairs authoritative state. Unit coverage separately proves unresolved lookups emit no status event.

## Commands

Fast tests without Docker:

```powershell
.\mvnw.cmd test
```

Complete verification with Docker Desktop running:

```powershell
.\mvnw.cmd clean verify
```

Run only the end-to-end module while also building its required service modules:

```powershell
.\mvnw.cmd verify -pl ledgerflow-e2e-tests -am
```

The first run downloads the Testcontainers dependencies and PostgreSQL/Kafka images. Testcontainers cleans up its ephemeral containers automatically after the test process exits.
