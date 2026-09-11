package io.ledgerflow.contracts;

import java.time.Instant;
import java.util.UUID;

public record TransactionStatusChangedEvent(
        UUID eventId,
        int schemaVersion,
        UUID transactionId,
        TransactionStatus status,
        String providerReference,
        String failureCode,
        String traceId,
        Instant occurredAt) {
}

