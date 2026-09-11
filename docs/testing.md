# Testing LedgerFlow

## Test layers

| Layer | Maven phase | Docker | Purpose |
| --- | --- | --- | --- |
| Unit | `test` | No | Fast domain and component behavior |
| End-to-end | `integration-test` / `verify` | Yes | Real PostgreSQL, Kafka, HTTP, and all three applications |

The `ledgerflow-e2e-tests` module is last in the reactor and uses Maven Failsafe, so its `*IT` tests run during `verify` rather than Surefire's unit-test phase.

## End-to-end topology

`TransactionFlowIT` starts ephemeral PostgreSQL and Kafka containers on random host ports. It then boots the provider simulator, processor, and transaction API in the test JVM on random web ports. No manually running Compose services or fixed ports are required.

The test verifies:

1. A new request returns `202 Accepted` in `PENDING` state.
2. The outbox publisher delivers the requested event through Kafka.
3. The processor calls the real provider simulator.
4. Status events produce exactly `PENDING → PROCESSING → COMPLETED` history.
5. The provider reference is retained.
6. Reusing the same idempotency key and payload returns `200 OK`, the same transaction ID, and the same provider reference.
7. PostgreSQL contains one transaction and one published outbox record.

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
