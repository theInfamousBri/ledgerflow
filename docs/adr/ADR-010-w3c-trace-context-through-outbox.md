# ADR-010: Preserve W3C trace context through the transactional outbox

## Status

Accepted

## Context

Spring can automatically propagate trace context across synchronous HTTP calls and observed Kafka producers/consumers. LedgerFlow’s initial Kafka publication is intentionally delayed, however: the API transaction commits an outbox row and a scheduler publishes it later on another thread. Thread-local trace context from the originating HTTP request no longer exists when that scheduler runs.

Allowing the outbox publisher to start an unrelated trace would hide the relationship between transaction acceptance and asynchronous processing. Publishing directly from the request thread would restore tracing continuity but reintroduce the database/Kafka dual-write failure window.

## Decision

Persist the W3C `traceparent` and optional `tracestate` values beside each outbox row in the same PostgreSQL transaction as the business event. When publishing, extract that context, create a `ledgerflow.outbox.publish` span with the stored remote parent, and explicitly inject the current context into the Kafka record headers.

The transaction API does not rely on `KafkaTemplate` observation to rediscover context after the scheduled outbox boundary. The publisher owns restoration and injection because it understands the persisted context. Enable Micrometer Observation on Kafka listener containers and on the processor's Kafka template for subsequent in-process publications. Use Spring Boot’s auto-configured `RestClient.Builder` for provider calls so HTTP propagation remains automatic. Export sampled spans through OTLP to an OpenTelemetry Collector, which forwards them to Jaeger locally. Include application name, trace ID, and span ID in log correlation prefixes.

## Consequences

A transaction can be followed across HTTP, the delayed outbox boundary, Kafka, the processor, and the provider under one trace ID. Trace context is operational metadata rather than business state: missing or malformed context starts a new publication trace, and an unavailable collector does not roll back transactions or Kafka work.

The local environment samples every trace to make demonstrations deterministic. Production environments must choose a lower sampling probability appropriate to traffic, cost, and incident-response needs. Outbox cleanup removes persisted trace context with the event after retention.
