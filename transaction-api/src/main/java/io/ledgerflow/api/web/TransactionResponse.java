package io.ledgerflow.api.web;

import io.ledgerflow.contracts.TransactionStatus;
import io.ledgerflow.contracts.TransactionType;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record TransactionResponse(
        UUID id,
        String accountId,
        BigDecimal amount,
        String currency,
        TransactionType type,
        TransactionStatus status,
        String providerReference,
        String failureCode,
        Instant createdAt,
        Instant updatedAt,
        List<HistoryItem> history) {

    public record HistoryItem(TransactionStatus status, String reason, Instant occurredAt) {}
}

