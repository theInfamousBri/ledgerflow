package io.ledgerflow.contracts;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record TransactionRequestedEvent(
        UUID eventId,
        int schemaVersion,
        UUID transactionId,
        String accountId,
        BigDecimal amount,
        String currency,
        TransactionType type,
        String traceId,
        Instant occurredAt) {
}
