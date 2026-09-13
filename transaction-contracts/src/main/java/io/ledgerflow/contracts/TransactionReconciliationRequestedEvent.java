package io.ledgerflow.contracts;

import java.time.Instant;
import java.util.UUID;

public record TransactionReconciliationRequestedEvent(
        UUID eventId,
        int schemaVersion,
        UUID transactionId,
        String traceId,
        Instant occurredAt) {
}
