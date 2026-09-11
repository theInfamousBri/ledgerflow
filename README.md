# LedgerFlow

LedgerFlow is a production-minded transaction-processing platform built to demonstrate reliable asynchronous backend design with Java 21, Spring Boot, Kafka, and PostgreSQL.

## What exists in this skeleton

- `transaction-api`: accepts idempotent transaction requests, owns authoritative state, records status history, and writes an outbox event in the same database transaction.
- `transaction-processor`: consumes requested events, calls an idempotent provider API, and emits processing/result events.
- `payment-provider-simulator`: deterministic idempotent stand-in for an unreliable downstream provider.
- `transaction-contracts`: versioned event and provider DTOs shared during the first development phase.
- `ledgerflow-e2e-tests`: boots the three applications against ephemeral PostgreSQL and Kafka containers and verifies the complete transaction lifecycle.
- `docs`: technical specification, architecture, and ADRs.

PostgreSQL is the source of record. Redis is deliberately deferred until a measured caching or coordination use case exists.

## Local prerequisites

- Java 21
- Maven 3.9+
- Docker with Compose

## Run

```bash
docker compose up --build
```

Create a transaction:

```bash
curl -i http://localhost:8080/transactions \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: abc-123' \
  -d '{"accountId":"acct-42","amount":125.50,"currency":"USD","type":"PAYMENT"}'
```

The API returns `202 Accepted` and a transaction ID. Retrieve it with:

```bash
curl http://localhost:8080/transactions/{transactionId}
```

Health endpoints are available at `/actuator/health` on ports 8080, 8081, and 8082.

## Build

Linux/macOS:

```bash
./mvnw clean verify
```

Windows PowerShell:

```powershell
.\mvnw.cmd clean verify
```

`verify` requires a running Docker engine because the Failsafe integration-test phase starts isolated PostgreSQL and Kafka containers. Unit tests alone can be run without Docker:

```bash
./mvnw test
```

See [the testing guide](docs/testing.md) for the end-to-end topology and debugging commands.

## Delivery roadmap

1. Harden API and consumer idempotency with concurrent and duplicate-delivery tests.
2. Add circuit breaking and deterministic retry/DLQ failure tests.
3. Add reconciliation and outbox cleanup/monitoring.
4. Add Redis only for justified acceleration or coordination.
5. Add OpenTelemetry, Prometheus, Grafana, and trace examples.
6. Add Kubernetes manifests, IaC, and measured load tests.

See [the technical specification](docs/technical-specification.md) for the precise contracts and invariants.

